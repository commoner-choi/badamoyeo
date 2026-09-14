package badamoyeo_api.ingestion.fetch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 이 설계의 핵심 불변식을 고정한다.
 *
 * <p>공공 API 는 임의 구간이 아니라 (pageNo, numOfRows) 로만 요청할 수 있다.
 * 구간 [from, to] 이 표현 가능하려면 {@code (from - 1) % size == 0} 이어야 한다.
 * 블록 크기가 2의 거듭제곱이면 아무리 쪼개도 이 조건이 유지된다는 것이 이 테스트의 주장이다.
 */
class BlockAlignmentTest {

	@Test
	@DisplayName("2의 거듭제곱 블록은 끝까지 쪼개도 항상 pageNo/numOfRows 로 표현된다")
	void powerOfTwoSplitsStayAligned() {
		assertAlignedAfterSplits(0, 256);
		assertAlignedAfterSplits(256, 256);
		assertAlignedAfterSplits(1024, 256);
	}

	private void assertAlignedAfterSplits(int offset, int size) {
		if (size == 1) {
			assertThat(offset % size).isZero();
			return;
		}
		assertThat(offset % size)
			.as("offset=%d size=%d 는 pageNo=%d 로 표현 가능해야 한다", offset, size, offset / size + 1)
			.isZero();
		int half = size / 2;
		assertAlignedAfterSplits(offset, half);
		assertAlignedAfterSplits(offset + half, half);
	}

	@Test
	@DisplayName("300으로 시작하면 세 번째 분할에서 정렬이 깨진다 - 기존 사다리가 실패 구간을 못 좁히던 이유")
	void threeHundredBreaksAlignment() {
		int size = 300 / 2 / 2;        // 300 -> 150 -> 75
		int secondHalfOffset = 75 / 2; // 75 를 37/38 로 쪼갠 오른쪽 조각의 시작 오프셋

		assertThat(size).isEqualTo(75);
		assertThat(secondHalfOffset % (size - secondHalfOffset))
			.as("37/38 로 쪼갠 오른쪽 조각은 어떤 (pageNo, numOfRows) 로도 표현할 수 없다")
			.isNotZero();
	}

	@Test
	@DisplayName("블록 크기는 offset 을 나누어떨어지게 하는 값까지 자동으로 줄어든다")
	void alignedSizeClampsToDivisor() {
		assertThat(BlockFetcher.alignedSize(0, 256)).isEqualTo(256);
		assertThat(BlockFetcher.alignedSize(512, 256)).isEqualTo(256);
		assertThat(BlockFetcher.alignedSize(384, 256)).isEqualTo(128);
		assertThat(BlockFetcher.alignedSize(128, 256)).isEqualTo(128);
		assertThat(BlockFetcher.alignedSize(64, 256)).isEqualTo(64);
	}
}
