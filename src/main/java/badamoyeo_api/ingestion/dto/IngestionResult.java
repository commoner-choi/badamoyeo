package badamoyeo_api.ingestion.dto;

import java.util.List;

/**
 * 카테고리 하나의 수집 결과.
 *
 * <p>기존에는 {@code experience/fetchedCount/savedCount} 뿐이라 "몇 건 저장했다"는 말은 할 수 있어도
 * "받아야 할 것 중 몇 %를 받았는가", "재시도와 분할이 몇 번 일어났는가"에 답할 수 없었다.
 * 수집 품질을 수치로 남기기 위해 측정값을 함께 싣는다.
 */
public record IngestionResult(
	String experience,
	int totalCount,
	int fetchedCount,
	int savedCount,
	int requestCount,
	int retryCount,
	int splitCount,
	List<FailedRange> failedRanges
) {

	public static IngestionResult empty(String experience, String reason) {
		return new IngestionResult(experience, 0, 0, 0, 0, 0, 0,
			List.of(new FailedRange(1, 0, reason)));
	}

	public boolean complete() {
		return failedRanges.isEmpty();
	}

	/** 서버가 알려준 전체 건수 대비 실제로 받아낸 비율. 수집 성공률 지표로 그대로 쓴다. */
	public double coverage() {
		return totalCount <= 0 ? 1.0 : (double) fetchedCount / totalCount;
	}

	public int lostRowCount() {
		return failedRanges.stream().mapToInt(range -> Math.max(range.size(), 0)).sum();
	}
}
