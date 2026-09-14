package badamoyeo_api.ingestion.fetch;

/**
 * 블록 크기를 성공/실패 피드백으로 조절한다 (TCP 혼잡제어의 AIMD와 같은 원리).
 *
 * <p>기존 구현은 페이지마다, 카테고리마다, 실행마다 항상 300에서 다시 시작했다.
 * API가 종일 불안정한 날에는 모든 페이지가 "큰 요청 실패 → 축소" 비용을 처음부터 다시 냈다.
 * 여기서는 직전 결과를 기억해 다음 블록의 출발 크기를 정한다.
 *
 * <p><b>크기가 2의 거듭제곱인 이유</b>: 이 API는 임의 구간이 아니라 {@code pageNo}/{@code numOfRows}
 * 로만 요청할 수 있다. 구간 [from, to] 는 {@code (from-1) % size == 0} 일 때만 표현 가능하다.
 * 300 → 150 → 75 → 37/38 로 쪼개면 37에서 정렬이 깨져 요청 자체를 만들 수 없다.
 * 크기를 2의 거듭제곱으로 두면 블록은 항상 {@code offset % size == 0} 을 유지하므로
 * 몇 번을 반으로 쪼개도 언제나 정확히 하나의 (pageNo, numOfRows) 로 표현된다.
 */
public final class AdaptiveBlockSize {

	public static final int DEFAULT_MAX = 256;
	public static final int DEFAULT_MIN = 1;
	private static final int GROW_AFTER_CLEAN_BLOCKS = 4;

	private final int min;
	private final int max;
	private int current;
	private int cleanStreak;

	public AdaptiveBlockSize(int min, int max, int initial) {
		if (!isPowerOfTwo(min) || !isPowerOfTwo(max) || !isPowerOfTwo(initial)) {
			throw new IllegalArgumentException("block sizes must be powers of two: min=" + min + ", max=" + max + ", initial=" + initial);
		}
		if (min > max || initial < min || initial > max) {
			throw new IllegalArgumentException("initial must be within [min, max]");
		}
		this.min = min;
		this.max = max;
		this.current = initial;
	}

	public static AdaptiveBlockSize defaults() {
		return new AdaptiveBlockSize(DEFAULT_MIN, DEFAULT_MAX, DEFAULT_MAX);
	}

	public int current() {
		return current;
	}

	/** 분할 없이 받아낸 블록. 연속으로 쌓이면 크기를 두 배로 되돌린다(가산적 회복). */
	public void onCleanBlock() {
		cleanStreak++;
		if (cleanStreak >= GROW_AFTER_CLEAN_BLOCKS && current < max) {
			current = Math.min(current * 2, max);
			cleanStreak = 0;
		}
	}

	/** 분할이 발생한 블록. 즉시 절반으로 줄인다(승산적 감소). */
	public void onSplit() {
		cleanStreak = 0;
		current = Math.max(current / 2, min);
	}

	static boolean isPowerOfTwo(int value) {
		return value > 0 && (value & (value - 1)) == 0;
	}
}
