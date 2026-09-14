package badamoyeo_api.ingestion.fetch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AdaptiveBlockSizeTest {

	@Test
	@DisplayName("분할이 일어나면 블록 크기를 절반으로 줄인다")
	void halvesOnSplit() {
		AdaptiveBlockSize size = new AdaptiveBlockSize(1, 256, 256);

		size.onSplit();

		assertThat(size.current()).isEqualTo(128);
	}

	@Test
	@DisplayName("최소 크기 아래로는 줄지 않는다")
	void doesNotShrinkBelowMin() {
		AdaptiveBlockSize size = new AdaptiveBlockSize(16, 256, 16);

		size.onSplit();
		size.onSplit();

		assertThat(size.current()).isEqualTo(16);
	}

	@Test
	@DisplayName("분할 없는 블록이 연속으로 쌓이면 크기를 두 배로 회복한다")
	void growsBackAfterCleanBlocks() {
		AdaptiveBlockSize size = new AdaptiveBlockSize(1, 256, 256);
		size.onSplit();
		assertThat(size.current()).isEqualTo(128);

		for (int i = 0; i < 4; i++) {
			size.onCleanBlock();
		}

		assertThat(size.current()).isEqualTo(256);
	}

	@Test
	@DisplayName("최대 크기를 넘겨 자라지 않는다")
	void doesNotGrowBeyondMax() {
		AdaptiveBlockSize size = new AdaptiveBlockSize(1, 64, 64);

		for (int i = 0; i < 20; i++) {
			size.onCleanBlock();
		}

		assertThat(size.current()).isEqualTo(64);
	}

	@Test
	@DisplayName("2의 거듭제곱이 아닌 크기는 만들 수 없다 - 분할 정렬이 깨지기 때문")
	void rejectsNonPowerOfTwo() {
		assertThatThrownBy(() -> new AdaptiveBlockSize(1, 300, 300))
			.isInstanceOf(IllegalArgumentException.class);
	}
}
