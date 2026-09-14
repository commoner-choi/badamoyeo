package badamoyeo_api.ingestion.fetch;

import java.net.URI;

/**
 * 블록 단위 수집기가 카테고리별 URL 규칙을 몰라도 되도록 분리한 지점.
 * 수집기는 (pageNo, numOfRows) 만 알고, 실제 주소 구성은 호출부가 담당한다.
 */
@FunctionalInterface
public interface PageUriFactory {
	URI create(int pageNo, int numOfRows);
}
