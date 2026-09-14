package badamoyeo_api.ingestion.fetch;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 지수 백오프 + full jitter 재시도 간격.
 *
 * <p>고정 간격(기존 500ms 고정)은 상대 서버가 과부하일 때 모든 재시도가 같은 리듬으로 몰린다.
 * 지수적으로 늘리되 0~계산값 사이에서 무작위로 뽑아(full jitter) 재시도가 한 시점에 겹치지 않게 한다.
 */
public record RetryPolicy(int maxAttempts, Duration baseDelay, Duration maxDelay, double multiplier) {

	public static RetryPolicy defaults() {
		return new RetryPolicy(3, Duration.ofMillis(500), Duration.ofSeconds(8), 2.0);
	}

	public RetryPolicy {
		if (maxAttempts < 1) {
			throw new IllegalArgumentException("maxAttempts must be >= 1");
		}
	}

	/**
	 * @param attempt 1부터 시작하는 시도 번호
	 * @param retryAfter 서버가 알려준 대기 시간(없으면 null). 있으면 그 값을 우선한다.
	 */
	public Duration delay(int attempt, Duration retryAfter) {
		if (retryAfter != null && !retryAfter.isNegative() && !retryAfter.isZero()) {
			return cap(retryAfter);
		}
		double exponential = baseDelay.toMillis() * Math.pow(multiplier, attempt - 1.0);
		long ceiling = Math.min((long) exponential, maxDelay.toMillis());
		if (ceiling <= 0) {
			return Duration.ZERO;
		}
		return Duration.ofMillis(ThreadLocalRandom.current().nextLong(ceiling + 1));
	}

	private Duration cap(Duration value) {
		return value.compareTo(maxDelay) > 0 ? maxDelay : value;
	}
}
