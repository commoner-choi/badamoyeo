package badamoyeo_api.ingestion.fetch;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import badamoyeo_api.ingestion.dto.FailedRange;

class BlockFetcherTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final RetryPolicy NO_WAIT = new RetryPolicy(2, Duration.ZERO, Duration.ZERO, 1.0);

	@Test
	@DisplayName("장애가 없으면 분할 없이 전체를 받아낸다")
	void fetchesEverythingWhenHealthy() {
		FakeApi api = new FakeApi(1000, row -> false);
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 400);
		List<JsonNode> collected = new ArrayList<>();

		BlockFetchOutcome outcome = fetcher.fetchAll(api::uri, "healthy", collected::addAll);

		assertThat(outcome.fetchedCount()).isEqualTo(1000);
		assertThat(collected).hasSize(1000);
		assertThat(outcome.splitCount()).isZero();
		assertThat(outcome.complete()).isTrue();
		assertThat(outcome.coverage()).isEqualTo(1.0);
	}

	@Test
	@DisplayName("손상된 행 1건이 있어도 나머지는 모두 살아남는다 - 기존 구현은 카테고리 전체를 잃었다")
	void losesOnlyTheBrokenRow() {
		FakeApi api = new FakeApi(300, row -> row == 137);
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 400);
		List<JsonNode> collected = new ArrayList<>();

		BlockFetchOutcome outcome = fetcher.fetchAll(api::uri, "one-bad-row", collected::addAll);

		assertThat(outcome.fetchedCount())
			.as("300건 중 문제 1건만 잃어야 한다")
			.isEqualTo(299);
		assertThat(collected).hasSize(299);
		assertThat(outcome.lostRowCount()).isEqualTo(1);
		assertThat(outcome.failedRanges())
			.singleElement()
			.extracting(FailedRange::fromRow, FailedRange::toRow)
			.containsExactly(137, 137);
	}

	@Test
	@DisplayName("문제 행을 log2(n)에 가까운 요청 수로 좁힌다")
	void isolatesBrokenRowInLogarithmicRequests() {
		FakeApi api = new FakeApi(256, row -> row == 200);
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 400);

		BlockFetchOutcome outcome = fetcher.fetchAll(api::uri, "isolate", items -> { });

		assertThat(outcome.splitCount())
			.as("256 -> 1 은 8단계이므로 분할은 8회를 넘지 않아야 한다")
			.isLessThanOrEqualTo(8);
		assertThat(outcome.fetchedCount()).isEqualTo(255);
	}

	@Test
	@DisplayName("일시적 실패는 재시도로 회복하고 분할까지 가지 않는다")
	void recoversByRetryWithoutSplitting() {
		FakeApi api = new FakeApi(256, row -> false);
		api.failFirstAttempts(1);
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 400);

		BlockFetchOutcome outcome = fetcher.fetchAll(api::uri, "flaky", items -> { });

		assertThat(outcome.retryCount()).isPositive();
		assertThat(outcome.splitCount()).isZero();
		assertThat(outcome.fetchedCount()).isEqualTo(256);
	}

	@Test
	@DisplayName("재시도해도 소용없는 실패는 즉시 포기하고 다음으로 넘어간다")
	void doesNotRetryPermanentFailures() {
		FakeApi api = new FakeApi(256, row -> false);
		api.failPermanently();
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 400);

		BlockFetchOutcome outcome = fetcher.fetchAll(api::uri, "permanent", items -> { });

		assertThat(outcome.retryCount()).isZero();
		assertThat(outcome.splitCount()).isZero();
		assertThat(outcome.fetchedCount()).isZero();
		assertThat(outcome.complete()).isFalse();
	}

	@Test
	@DisplayName("호출량 제한은 재시도는 하되 분할하지 않는다 - 쪼개면 요청이 늘어 상황이 악화된다")
	void doesNotSplitOnRateLimit() {
		FakeApi api = new FakeApi(256, row -> false);
		api.failWith(MarineFetchException.Kind.RATE_LIMITED);
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 400);

		BlockFetchOutcome outcome = fetcher.fetchAll(api::uri, "rate-limited", items -> { });

		assertThat(outcome.splitCount()).isZero();
		assertThat(outcome.retryCount()).isPositive();
	}

	@Test
	@DisplayName("성공한 블록은 실패한 블록보다 먼저 저장되어 남는다")
	void savesSuccessfulBlocksEvenWhenLaterBlocksFail() {
		FakeApi api = new FakeApi(512, row -> row > 300);
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 400);
		AtomicInteger savedBatches = new AtomicInteger();
		List<JsonNode> collected = new ArrayList<>();

		fetcher.fetchAll(api::uri, "partial", items -> {
			savedBatches.incrementAndGet();
			collected.addAll(items);
		});

		assertThat(savedBatches.get()).isPositive();
		assertThat(collected).hasSize(300);
	}

	@Test
	@DisplayName("요청 예산을 넘기면 분할을 멈춘다")
	void stopsWhenRequestBudgetIsExhausted() {
		FakeApi api = new FakeApi(1024, row -> row % 2 == 0);
		BlockFetcher fetcher = new BlockFetcher(api, NO_WAIT, 30);

		BlockFetchOutcome outcome = fetcher.fetchAll(api::uri, "budget", items -> { });

		assertThat(outcome.requestCount()).isLessThanOrEqualTo(30);
		assertThat(outcome.complete()).isFalse();
	}

	/**
	 * 행 번호 기준으로 "이 행이 포함되면 응답 생성에 실패하는" API 를 흉내낸다.
	 * 요청 정렬이 깨지면 여기서 바로 드러나도록 (pageNo, numOfRows) 조합을 검사한다.
	 */
	private static final class FakeApi extends MarineApiClient {
		private final int totalCount;
		private final Predicate<Integer> brokenRow;
		private final Set<String> seen = new java.util.HashSet<>();
		private int failFirstAttempts;
		private int attempts;
		private MarineFetchException.Kind alwaysFailWith;

		private FakeApi(int totalCount, Predicate<Integer> brokenRow) {
			super(MAPPER);
			this.totalCount = totalCount;
			this.brokenRow = brokenRow;
		}

		private void failFirstAttempts(int count) {
			this.failFirstAttempts = count;
		}

		private void failPermanently() {
			this.alwaysFailWith = MarineFetchException.Kind.PERMANENT;
		}

		private void failWith(MarineFetchException.Kind kind) {
			this.alwaysFailWith = kind;
		}

		private URI uri(int pageNo, int numOfRows) {
			return URI.create("https://example.test/forecast?pageNo=" + pageNo + "&numOfRows=" + numOfRows);
		}

		@Override
		public FetchPage fetch(URI uri) {
			int pageNo = Integer.parseInt(param(uri, "pageNo"));
			int numOfRows = Integer.parseInt(param(uri, "numOfRows"));
			int from = (pageNo - 1) * numOfRows + 1;

			if (alwaysFailWith != null) {
				throw new MarineFetchException(alwaysFailWith, "forced " + alwaysFailWith);
			}
			if (++attempts <= failFirstAttempts) {
				throw new MarineFetchException(MarineFetchException.Kind.TRANSIENT, "flaky");
			}
			List<JsonNode> items = new ArrayList<>();
			for (int row = from; row < from + numOfRows && row <= totalCount; row++) {
				if (brokenRow.test(row)) {
					throw new MarineFetchException(MarineFetchException.Kind.OVERSIZED,
						"row " + row + " breaks the response");
				}
				items.add(MAPPER.createObjectNode().put("row", row));
			}
			if (!seen.add(pageNo + "/" + numOfRows)) {
				throw new IllegalStateException("같은 블록을 두 번 성공적으로 받아왔다 - 정렬이 깨졌다는 뜻: " + uri);
			}
			return new FetchPage(items, totalCount);
		}

		private String param(URI uri, String name) {
			for (String pair : uri.getQuery().split("&")) {
				String[] kv = pair.split("=", 2);
				if (kv[0].equals(name)) {
					return kv[1];
				}
			}
			throw new IllegalArgumentException("missing param " + name);
		}
	}
}
