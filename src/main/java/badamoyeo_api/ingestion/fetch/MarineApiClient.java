package badamoyeo_api.ingestion.fetch;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 공공 API 한 번 호출 + 응답 해석. 모든 실패를 {@link MarineFetchException} 으로 분류해서 올린다.
 *
 * <p>여기서 타임아웃을 명시하는 이유: 기존에는 {@code RestClient.create()} 를 그대로 써서
 * 읽기 타임아웃이 없었고, 응답이 오지 않으면 6시간 주기 스케줄러가 다음 주기까지 매달릴 수 있었다.
 */
@Component
public class MarineApiClient {

	private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
	private static final Duration READ_TIMEOUT = Duration.ofSeconds(20);

	private final RestClient restClient;
	private final ObjectMapper objectMapper;

	public MarineApiClient(ObjectMapper objectMapper) {
		this(objectMapper, defaultRestClient());
	}

	MarineApiClient(ObjectMapper objectMapper, RestClient restClient) {
		this.objectMapper = objectMapper;
		this.restClient = restClient;
	}

	private static RestClient defaultRestClient() {
		JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(
			HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build());
		factory.setReadTimeout(READ_TIMEOUT);
		return RestClient.builder().requestFactory(factory).build();
	}

	public FetchPage fetch(URI uri) {
		String body = readBody(uri);
		JsonNode root = parse(body);
		JsonNode envelope = root.has("response") ? root.path("response") : root;
		JsonNode header = envelope.path("header");
		String resultCode = header.path("resultCode").asText();
		if (!"00".equals(resultCode)) {
			throw classifyResultCode(resultCode, header.path("resultMsg").asText("open api request failed"));
		}
		JsonNode payload = envelope.path("body");
		return new FetchPage(asArray(payload.path("items").path("item")), payload.path("totalCount").asInt(0));
	}

	private String readBody(URI uri) {
		String body;
		try {
			body = restClient.get().uri(uri).retrieve().body(String.class);
		} catch (RestClientResponseException exception) {
			throw classifyHttpStatus(exception);
		} catch (ResourceAccessException exception) {
			// 연결 실패 / 읽기 타임아웃 / 소켓 끊김. 기존 구현이 놓쳐서 수집 전체를 중단시키던 경로다.
			throw new MarineFetchException(MarineFetchException.Kind.OVERSIZED,
				"open api io failure: " + exception.getMessage(), null, exception);
		}
		if (body == null || body.isBlank()) {
			throw new MarineFetchException(MarineFetchException.Kind.TRANSIENT, "empty open api response");
		}
		return body;
	}

	private MarineFetchException classifyHttpStatus(RestClientResponseException exception) {
		HttpStatusCode status = exception.getStatusCode();
		String message = "open api http " + status.value() + ": " + sanitize(exception.getResponseBodyAsString());
		if (status.value() == 429) {
			return new MarineFetchException(MarineFetchException.Kind.RATE_LIMITED, message,
				parseRetryAfter(exception.getResponseHeaders()), exception);
		}
		if (status.value() == 413 || status.value() == 504) {
			return new MarineFetchException(MarineFetchException.Kind.OVERSIZED, message, null, exception);
		}
		if (status.is5xxServerError()) {
			return new MarineFetchException(MarineFetchException.Kind.TRANSIENT, message, null, exception);
		}
		return new MarineFetchException(MarineFetchException.Kind.PERMANENT, message, null, exception);
	}

	/**
	 * 공공데이터포털 표준 결과코드. 03(데이터 없음)은 실패가 아니라 빈 결과이므로 여기 오지 않는다.
	 * 22/30/31/32(호출 초과, 등록되지 않은 키, 기한 만료, 도메인 불일치)는 재시도해도 달라지지 않는다.
	 */
	private MarineFetchException classifyResultCode(String resultCode, String resultMsg) {
		String message = "open api resultCode=" + resultCode + ", resultMsg=" + resultMsg;
		return switch (resultCode) {
			case "99", "02" -> new MarineFetchException(MarineFetchException.Kind.TRANSIENT, message);
			case "22" -> new MarineFetchException(MarineFetchException.Kind.RATE_LIMITED, message);
			default -> new MarineFetchException(MarineFetchException.Kind.PERMANENT, message);
		};
	}

	private Duration parseRetryAfter(HttpHeaders headers) {
		if (headers == null) {
			return null;
		}
		String value = headers.getFirst(HttpHeaders.RETRY_AFTER);
		if (value == null || value.isBlank()) {
			return null;
		}
		try {
			return Duration.ofSeconds(Long.parseLong(value.trim()));
		} catch (NumberFormatException exception) {
			return null;
		}
	}

	private JsonNode parse(String body) {
		try {
			return objectMapper.readTree(body);
		} catch (Exception exception) {
			// 응답이 잘려 들어온 경우가 많아 일시적 실패로 본다. 분할하면 작은 응답이라 성공하기도 한다.
			throw new MarineFetchException(MarineFetchException.Kind.TRANSIENT,
				"invalid open api response: " + sanitize(body), null, exception);
		}
	}

	private List<JsonNode> asArray(JsonNode items) {
		if (items == null || items.isMissingNode() || items.isNull()) {
			return List.of();
		}
		if (!items.isArray()) {
			return List.of(items);
		}
		List<JsonNode> nodes = new ArrayList<>();
		items.forEach(nodes::add);
		return nodes;
	}

	private String sanitize(String body) {
		if (body == null || body.isBlank()) {
			return "";
		}
		String compact = body.replaceAll("\s+", " ").trim();
		return compact.length() > 200 ? compact.substring(0, 200) : compact;
	}
}
