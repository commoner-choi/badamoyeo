package badamoyeo_api.ingestion.fetch;

import java.util.List;

import badamoyeo_api.ingestion.dto.FailedRange;

/**
 * 한 카테고리 수집을 끝낸 뒤의 측정값.
 *
 * <p>기존 구현은 성공/실패가 로그로만 남아 "수집 성공률이 얼마인가"에 답할 수 없었다.
 * 수집기가 스스로 세어서 값으로 돌려준다.
 */
public record BlockFetchOutcome(
	int totalCount,
	int fetchedCount,
	int requestCount,
	int retryCount,
	int splitCount,
	List<FailedRange> failedRanges
) {

	public boolean complete() {
		return failedRanges.isEmpty();
	}

	/** 서버가 알려준 전체 건수 대비 실제로 받아낸 비율. */
	public double coverage() {
		return totalCount <= 0 ? 1.0 : (double) fetchedCount / totalCount;
	}

	public int lostRowCount() {
		return failedRanges.stream().mapToInt(FailedRange::size).sum();
	}
}
