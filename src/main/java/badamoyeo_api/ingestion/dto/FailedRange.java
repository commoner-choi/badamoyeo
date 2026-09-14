package badamoyeo_api.ingestion.dto;

/**
 * 끝내 받아내지 못한 행 구간. 1부터 시작하는 닫힌 구간 [fromRow, toRow] 이다.
 *
 * <p>예외를 던져 카테고리 전체를 중단시키는 대신 이 구간만 남긴다.
 * 덕분에 나머지 구간은 계속 수집되고, 실패 지점은 로그가 아니라 값으로 남아
 * 다음 실행에서 이 구간만 다시 시도할 수 있다.
 */
public record FailedRange(int fromRow, int toRow, String reason) {

	public int size() {
		return toRow - fromRow + 1;
	}

	@Override
	public String toString() {
		return "[" + fromRow + "-" + toRow + "] " + reason;
	}
}
