package badamoyeo_api.ingestion.fetch;

import java.time.Duration;

/**
 * 해양예보 공공 API 호출 실패를 원인별로 구분한다.
 *
 * <p>기존 구현은 실패 판정을 {@code getReason().contains("resultCode=99")} 처럼 문자열 매칭으로 했고,
 * 그래서 메시지 포맷이 바뀌면 조용히 재시도가 꺼졌다. 또 네트워크 타임아웃은 아예 분류 대상이 아니어서
 * 재시도 경로에 닿지도 못한 채 수집 전체를 중단시켰다. 원인을 타입으로 승격해 그 두 문제를 함께 막는다.
 */
public class MarineFetchException extends RuntimeException {

	public enum Kind {
		/** 5xx, resultCode=99 등 일시적 서버 오류. 재시도 후 안 되면 분할한다. */
		TRANSIENT,
		/** 연결/읽기 타임아웃, 413, 504 등 "응답이 너무 크다"에 가까운 실패. 분할이 가장 효과적이다. */
		OVERSIZED,
		/** 429 등 호출량 제한. 재시도는 하되 <b>분할하면 안 된다</b> — 요청 수가 늘어 상황을 악화시킨다. */
		RATE_LIMITED,
		/** 400, 401, 잘못된 서비스키 등. 재시도도 분할도 의미가 없으므로 즉시 포기한다. */
		PERMANENT
	}

	private final transient Kind kind;
	private final transient Duration retryAfter;

	public MarineFetchException(Kind kind, String message, Duration retryAfter, Throwable cause) {
		super(message, cause);
		this.kind = kind;
		this.retryAfter = retryAfter;
	}

	public MarineFetchException(Kind kind, String message) {
		this(kind, message, null, null);
	}

	public Kind kind() {
		return kind;
	}

	/** 서버가 {@code Retry-After} 로 알려준 대기 시간. 없으면 null. */
	public Duration retryAfter() {
		return retryAfter;
	}

	public boolean retryable() {
		return kind != Kind.PERMANENT;
	}

	/**
	 * 구간을 반으로 쪼개 다시 시도할 가치가 있는가.
	 * RATE_LIMITED 는 재시도 대상이지만 분할 대상은 아니다 — 쪼개면 요청 수가 늘기 때문이다.
	 */
	public boolean splittable() {
		return kind == Kind.TRANSIENT || kind == Kind.OVERSIZED;
	}
}
