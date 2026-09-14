package badamoyeo_api.ingestion.fetch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import badamoyeo_api.ingestion.dto.FailedRange;

/**
 * 한 카테고리 전체를 블록 단위로 받아낸다. 실패한 블록은 반으로 쪼개 실패 구간을 좁힌다.
 *
 * <p><b>왜 단계적 축소(300→100→50→10)가 아니라 이분 분할인가</b>
 * <ul>
 *   <li>단계적 축소는 300건 요청이 실패하면 같은 1~300 구간을 100짜리 3개로 <i>전부 다시</i> 요청했다.
 *       실패 구간을 좁히는 게 아니라 구간 전체를 재분할하는 것이라, 문제 행 1건을 찾는 데 드는
 *       요청 수가 구간 크기에 비례했다.</li>
 *   <li>또 재분할한 요청 중 하나만 실패해도 예외가 올라가면서 앞서 성공한 결과까지 함께 사라지고,
 *       그 예외가 카테고리 전체를 건너뛰게 만들었다. 손상된 행 1건에 299건을 잃는 구조였다.</li>
 *   <li>이분 분할은 성공한 절반을 즉시 확정하고 실패한 절반만 다시 쪼갠다. 문제 행 1건은
 *       약 log2(n)번의 요청으로 좁혀지고, 나머지 행은 그대로 남는다.</li>
 * </ul>
 *
 * <p><b>정렬 불변식</b>: 이 API는 임의 구간이 아니라 (pageNo, numOfRows) 로만 요청할 수 있다.
 * 블록 크기를 2의 거듭제곱으로 유지하고 {@code offset % size == 0} 을 지키면
 * 몇 번을 쪼개도 항상 정확히 하나의 (pageNo, numOfRows) 로 표현된다.
 * 크기가 홀수가 되는 순간(예: 75 → 37/38) 두 번째 조각은 요청 자체를 만들 수 없다.
 */
@Component
public class BlockFetcher {

	private static final Logger log = LoggerFactory.getLogger(BlockFetcher.class);

	/** 카테고리 하나가 쓸 수 있는 총 요청 수 상한. 분할이 곱셈으로 번지는 것을 막는다. */
	private static final int DEFAULT_REQUEST_BUDGET = 400;
	/** 분할 깊이 상한. 256 → 1 이 8단계이므로 그보다 넉넉히 둔다. */
	private static final int MAX_SPLIT_DEPTH = 12;

	private final MarineApiClient client;
	private final RetryPolicy retryPolicy;
	private final int requestBudget;

	public BlockFetcher(MarineApiClient client) {
		this(client, RetryPolicy.defaults(), DEFAULT_REQUEST_BUDGET);
	}

	BlockFetcher(MarineApiClient client, RetryPolicy retryPolicy, int requestBudget) {
		this.client = client;
		this.retryPolicy = retryPolicy;
		this.requestBudget = requestBudget;
	}

	/**
	 * @param sink 블록 하나를 받아낼 때마다 호출된다. 여기서 바로 저장하면
	 *             뒤쪽 블록이 실패해도 앞쪽 결과는 이미 커밋되어 남는다.
	 */
	public BlockFetchOutcome fetchAll(PageUriFactory uriFactory, String label, Consumer<List<JsonNode>> sink) {
		Session session = new Session(uriFactory, label, requestBudget);
		AdaptiveBlockSize blockSize = AdaptiveBlockSize.defaults();

		int offset = 0;
		while (session.totalCount < 0 || offset < session.totalCount) {
			int size = alignedSize(offset, blockSize.current());
			int splitsBefore = session.splitCount;

			List<JsonNode> items = fetchBlock(session, offset, size, 0);
			if (!items.isEmpty()) {
				sink.accept(items);
				session.fetchedCount += items.size();
			}

			if (session.splitCount > splitsBefore) {
				blockSize.onSplit();
			} else {
				blockSize.onCleanBlock();
			}

			if (session.totalCount < 0) {
				// 첫 블록조차 전부 실패해 전체 건수를 모른다. 더 진행할 근거가 없으므로 멈춘다.
				log.warn("Abort ingestion block loop because totalCount is unknown. label={}", label);
				break;
			}
			if (session.budgetExhausted()) {
				session.failedRanges.add(new FailedRange(offset + size + 1, Math.max(session.totalCount, offset + size + 1),
					"request budget exhausted"));
				log.warn("Stop ingestion because request budget is exhausted. label={}, requests={}", label, session.requestCount);
				break;
			}
			offset += size;
		}

		BlockFetchOutcome outcome = new BlockFetchOutcome(Math.max(session.totalCount, 0), session.fetchedCount,
			session.requestCount, session.retryCount, session.splitCount, List.copyOf(session.failedRanges));
		log.info("Finished ingestion blocks. label={}, totalCount={}, fetched={}, coverage={}, requests={}, retries={}, splits={}, lostRows={}",
			label, outcome.totalCount(), outcome.fetchedCount(), String.format("%.4f", outcome.coverage()),
			outcome.requestCount(), outcome.retryCount(), outcome.splitCount(), outcome.lostRowCount());
		return outcome;
	}

	/**
	 * offset 이 size 의 배수가 아니면 (pageNo, numOfRows) 로 표현할 수 없다.
	 * offset 을 나누어떨어지게 하는 가장 큰 2의 거듭제곱까지 줄여 불변식을 지킨다.
	 */
	static int alignedSize(int offset, int desired) {
		if (offset == 0) {
			return desired;
		}
		int largestDivisor = offset & -offset;
		return Math.min(desired, largestDivisor);
	}

	private List<JsonNode> fetchBlock(Session session, int offset, int size, int depth) {
		int pageNo = offset / size + 1;
		try {
			FetchPage page = fetchWithRetry(session, pageNo, size);
			session.totalCount = page.totalCount();
			return page.items();
		} catch (MarineFetchException exception) {
			return handleBlockFailure(session, offset, size, depth, exception);
		}
	}

	private List<JsonNode> handleBlockFailure(Session session, int offset, int size, int depth,
		MarineFetchException exception) {
		int fromRow = offset + 1;
		int toRow = offset + size;
		boolean canSplit = exception.splittable()
			&& size > 1
			&& depth < MAX_SPLIT_DEPTH
			&& !session.budgetExhausted();

		if (!canSplit) {
			session.failedRanges.add(new FailedRange(fromRow, toRow, describe(exception)));
			log.warn("Give up ingestion block. label={}, rows={}-{}, size={}, kind={}, reason={}",
				session.label, fromRow, toRow, size, exception.kind(), exception.getMessage());
			return List.of();
		}

		session.splitCount++;
		int half = size / 2;
		log.warn("Split ingestion block. label={}, rows={}-{}, size={} -> {}, kind={}, reason={}",
			session.label, fromRow, toRow, size, half, exception.kind(), exception.getMessage());

		List<JsonNode> merged = new ArrayList<>(fetchBlock(session, offset, half, depth + 1));
		// 오른쪽 절반이 전체 건수를 넘어섰다면 요청할 필요가 없다.
		if (session.totalCount < 0 || offset + half < session.totalCount) {
			merged.addAll(fetchBlock(session, offset + half, half, depth + 1));
		}
		return merged;
	}

	private FetchPage fetchWithRetry(Session session, int pageNo, int size) {
		MarineFetchException last = null;
		for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
			if (session.budgetExhausted()) {
				throw last != null ? last
					: new MarineFetchException(MarineFetchException.Kind.PERMANENT, "request budget exhausted");
			}
			session.requestCount++;
			try {
				return client.fetch(session.uriFactory.create(pageNo, size));
			} catch (MarineFetchException exception) {
				last = exception;
				if (!exception.retryable() || attempt == retryPolicy.maxAttempts()) {
					throw exception;
				}
				session.retryCount++;
				sleep(retryPolicy.delay(attempt, exception.retryAfter()));
			}
		}
		throw last != null ? last : new MarineFetchException(MarineFetchException.Kind.TRANSIENT, "unreachable");
	}

	private String describe(MarineFetchException exception) {
		return exception.kind() + ": " + exception.getMessage();
	}

	private void sleep(Duration duration) {
		if (duration == null || duration.isZero() || duration.isNegative()) {
			return;
		}
		try {
			Thread.sleep(duration.toMillis());
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new MarineFetchException(MarineFetchException.Kind.PERMANENT,
				"interrupted while waiting to retry", null, exception);
		}
	}

	private static final class Session {
		private final PageUriFactory uriFactory;
		private final String label;
		private final int budget;
		private final List<FailedRange> failedRanges = new ArrayList<>();
		private int totalCount = -1;
		private int fetchedCount;
		private int requestCount;
		private int retryCount;
		private int splitCount;

		private Session(PageUriFactory uriFactory, String label, int budget) {
			this.uriFactory = uriFactory;
			this.label = label;
			this.budget = budget;
		}

		private boolean budgetExhausted() {
			return requestCount >= budget;
		}
	}
}
