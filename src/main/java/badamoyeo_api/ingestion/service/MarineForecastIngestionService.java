package badamoyeo_api.ingestion.service;

import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import badamoyeo_api.ingestion.dto.ForecastUpsertRequest;
import badamoyeo_api.ingestion.dto.IngestionResult;
import badamoyeo_api.ingestion.dto.SpotIdLookup;
import badamoyeo_api.ingestion.dto.SpotUpsertRequest;
import badamoyeo_api.ingestion.fetch.BlockFetchOutcome;
import badamoyeo_api.ingestion.fetch.BlockFetcher;
import badamoyeo_api.ingestion.mapper.MarineForecastIngestionMapper;
import badamoyeo_api.spot.dto.Experience;

/**
 * 해양예보 공공데이터를 카테고리별로 수집해 스팟/예보 테이블에 반영한다.
 *
 * <p>HTTP 호출과 실패 복구는 {@link BlockFetcher} 에 있다. 이 클래스는 카테고리 목록을 돌면서
 * 받아낸 블록을 저장하고 측정값을 모으는 역할만 한다.
 */
@Service
public class MarineForecastIngestionService {
	private static final Logger log = LoggerFactory.getLogger(MarineForecastIngestionService.class);
	private static final DateTimeFormatter REQUEST_DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;

	private final MarineForecastIngestionMapper ingestionMapper;
	private final RegionResolver regionResolver;
	private final TransactionTemplate transactionTemplate;
	private final BlockFetcher blockFetcher;
	private final ObjectMapper objectMapper;
	private final String serviceKey;

	public MarineForecastIngestionService(
		MarineForecastIngestionMapper ingestionMapper,
		RegionResolver regionResolver,
		TransactionTemplate transactionTemplate,
		BlockFetcher blockFetcher,
		ObjectMapper objectMapper,
		@Value("${openapi.marine.service-key:}") String serviceKey
	) {
		this.ingestionMapper = ingestionMapper;
		this.regionResolver = regionResolver;
		this.transactionTemplate = transactionTemplate;
		this.blockFetcher = blockFetcher;
		this.objectMapper = objectMapper;
		this.serviceKey = serviceKey;
	}

	public List<IngestionResult> ingestAll(LocalDate targetDate) {
		if (serviceKey == null || serviceKey.isBlank()) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "openapi.marine.service-key is required");
		}

		LocalDate requestDate = targetDate == null ? LocalDate.now() : targetDate;
		List<IngestionResult> results = new ArrayList<>();
		for (ApiSpec spec : apiSpecs()) {
			// 한 카테고리에서 무엇이 터지든 다음 카테고리는 계속한다.
			// 예전에는 ResponseStatusException 만 잡아서 네트워크 타임아웃이 이 루프를 뚫고 나갔다.
			try {
				results.add(ingest(spec, requestDate));
			} catch (RuntimeException exception) {
				log.error("Skip marine forecast experience. experience={}, date={}",
					spec.experience().apiValue(), requestDate, exception);
				results.add(IngestionResult.empty(spec.experience().apiValue(), exception.toString()));
			}
		}
		logSummary(requestDate, results);
		return results;
	}

	private List<ApiSpec> apiSpecs() {
		return List.of(
			ApiSpec.seaTravel(),
			ApiSpec.swimming(),
			ApiSpec.fishing(),
			ApiSpec.mudflat(),
			ApiSpec.scuba(),
			ApiSpec.surfing()
		);
	}

	private IngestionResult ingest(ApiSpec spec, LocalDate requestDate) {
		String label = spec.experience().apiValue() + "@" + requestDate;
		int[] savedCount = {0};

		// 블록 하나를 받을 때마다 바로 저장한다. 뒤쪽 블록이 실패해도 여기까지는 이미 커밋되어 남는다.
		BlockFetchOutcome outcome = blockFetcher.fetchAll(
			(pageNo, numOfRows) -> forecastUri(spec, requestDate, pageNo, numOfRows),
			label,
			items -> savedCount[0] += saveItems(spec, items));

		return new IngestionResult(
			spec.experience().apiValue(),
			outcome.totalCount(),
			outcome.fetchedCount(),
			savedCount[0],
			outcome.requestCount(),
			outcome.retryCount(),
			outcome.splitCount(),
			outcome.failedRanges());
	}

	private void logSummary(LocalDate requestDate, List<IngestionResult> results) {
		int totalCount = results.stream().mapToInt(IngestionResult::totalCount).sum();
		int fetchedCount = results.stream().mapToInt(IngestionResult::fetchedCount).sum();
		int lostRows = results.stream().mapToInt(IngestionResult::lostRowCount).sum();
		long incomplete = results.stream().filter(result -> !result.complete()).count();
		double coverage = totalCount <= 0 ? 1.0 : (double) fetchedCount / totalCount;

		log.info("Marine forecast ingestion summary. date={}, coverage={}, totalCount={}, fetched={}, lostRows={}, incompleteExperiences={}/{}",
			requestDate, String.format("%.4f", coverage), totalCount, fetchedCount, lostRows, incomplete, results.size());

		for (IngestionResult result : results) {
			if (!result.complete()) {
				log.warn("Incomplete experience ingestion. experience={}, coverage={}, lostRows={}, failedRanges={}",
					result.experience(), String.format("%.4f", result.coverage()), result.lostRowCount(), result.failedRanges());
			}
		}
	}

	private URI forecastUri(ApiSpec spec, LocalDate requestDate, int pageNo, int pageSize) {
		String query = UriComponentsBuilder.newInstance()
			.queryParam("type", "json")
			.queryParam("reqDate", requestDate.format(REQUEST_DATE_FORMATTER))
			.queryParam("pageNo", pageNo)
			.queryParam("numOfRows", pageSize)
			.queryParams(spec.fixedParams())
			.build()
			.encode()
			.getQuery();
		String serviceKeyQuery = "serviceKey=" + encodeServiceKey(serviceKey);
		return URI.create("https://apis.data.go.kr/1192136/" + spec.path()
			+ "?" + serviceKeyQuery
			+ (query == null || query.isBlank() ? "" : "&" + query));
	}

	private String encodeServiceKey(String value) {
		if (value.contains("%")) {
			return value;
		}
		return UriUtils.encodeQueryParam(value, StandardCharsets.UTF_8);
	}

	private int saveItems(ApiSpec spec, List<JsonNode> items) {
		return transactionTemplate.execute(status -> saveItemsInTransaction(spec, items));
	}

	private int saveItemsInTransaction(ApiSpec spec, List<JsonNode> items) {
		if (items.isEmpty()) {
			return 0;
		}

		Map<String, SpotUpsertRequest> uniqueSpots = new LinkedHashMap<>();
		for (JsonNode item : items) {
			SpotUpsertRequest spotRequest = toSpotRequest(spec, item);
			uniqueSpots.put(spotKey(spotRequest.experience(), spotRequest.name()), spotRequest);
		}

		List<SpotUpsertRequest> spots = new ArrayList<>(uniqueSpots.values());
		ingestionMapper.upsertSpots(spots);

		Map<String, Long> spotIds = new LinkedHashMap<>();
		for (SpotIdLookup lookup : ingestionMapper.findSpotIds(spots)) {
			spotIds.put(spotKey(lookup.experience(), lookup.name()), lookup.spotId());
		}

		List<ForecastUpsertRequest> forecasts = new ArrayList<>();
		for (JsonNode item : items) {
			SpotUpsertRequest spotRequest = toSpotRequest(spec, item);
			Long spotId = spotIds.get(spotKey(spotRequest.experience(), spotRequest.name()));
			if (spotId == null) {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "failed to resolve spot id");
			}
			forecasts.add(toForecastRequest(spec, item, spotId));
		}

		ingestionMapper.upsertForecasts(forecasts);
		return forecasts.size();
	}

	private SpotUpsertRequest toSpotRequest(ApiSpec spec, JsonNode item) {
		String spotName = text(item, spec.nameField());
		BigDecimal lat = decimal(item, "lat");
		BigDecimal lng = decimal(item, "lot");

		return new SpotUpsertRequest(
			spec.experience().apiValue(),
			spotName,
			lat,
			lng,
			spec.placeCode(item),
			spotName,
			regionResolver.resolve(lat, lng)
		);
	}

	private ForecastUpsertRequest toForecastRequest(ApiSpec spec, JsonNode item, Long spotId) {
		LocalDate forecastDate = LocalDate.parse(text(item, "predcYmd"));
		String timeSlot = nullToEmpty(text(item, "predcNoonSeCd"));
		String totalIndex = text(item, "totalIndex");
		String weather = text(item, "weather");
		String tide = text(item, "tdlvHrCn");
		String variantKey = spec.variantKey(item);

		return new ForecastUpsertRequest(
			spotId,
			spec.experience().apiValue(),
			forecastDate,
			timeSlot,
			totalIndex,
			weather,
			tide,
			variantKey,
			toJson(spec.metrics(item)),
			toJson(item)
		);
	}

	private String spotKey(String experience, String name) {
		return experience + "\n" + name;
	}

	private String toJson(JsonNode node) {
		try {
			return objectMapper.writeValueAsString(node);
		} catch (Exception exception) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "failed to serialize json", exception);
		}
	}

	private String text(JsonNode item, String field) {
		JsonNode value = item.path(field);
		if (value.isMissingNode() || value.isNull()) {
			return null;
		}
		String text = value.asText();
		return text.isBlank() ? null : text;
	}

	private String nullToEmpty(String value) {
		return value == null ? "" : value;
	}

	private BigDecimal decimal(JsonNode item, String field) {
		String value = text(item, field);
		return value == null ? null : new BigDecimal(value);
	}

	private record ApiSpec(
		Experience experience,
		String path,
		String nameField,
		org.springframework.util.MultiValueMap<String, String> fixedParams,
		List<MetricField> metrics,
		String variantField
	) {
		static ApiSpec seaTravel() {
			return new ApiSpec(Experience.SEA_TRAVEL, "fcstSeaTripv2/GetFcstSeaTripApiServicev2", "sareaDtlNm",
				new org.springframework.util.LinkedMultiValueMap<>(),
				List.of(
					new MetricField("weather", "weather"),
					new MetricField("tide", "tdlvHrCn"),
					new MetricField("airTemperature", "avgArtmp"),
					new MetricField("windSpeed", "avgWspd"),
					new MetricField("waterTemperature", "avgWtem"),
					new MetricField("waveHeight", "avgWvhgt"),
					new MetricField("currentSpeed", "avgCrsp")
				),
				null
			);
		}

		static ApiSpec swimming() {
			return new ApiSpec(Experience.SWIMMING, "fcstBeachv2/GetFcstBeachApiServicev2", "bbchNm",
				new org.springframework.util.LinkedMultiValueMap<>(),
				List.of(
					new MetricField("openStatus", "opnStat"),
					new MetricField("waterTemperature", "avgWtem"),
					new MetricField("waveHeight", "maxWvhgt"),
					new MetricField("airTemperature", "avgArtmp"),
					new MetricField("windSpeed", "maxWspd")
				),
				null
			);
		}

		static ApiSpec fishing() {
			org.springframework.util.LinkedMultiValueMap<String, String> params = new org.springframework.util.LinkedMultiValueMap<>();
			params.add("gubun", "갯바위");
			return new ApiSpec(Experience.FISHING, "fcstFishingv2/GetFcstFishingApiServicev2", "seafsPstnNm",
				params,
				List.of(
					new MetricField("targetFish", "seafsTgfshNm"),
					new MetricField("tide", "tdlvHrCn"),
					new MetricField("waterTemperatureMin", "minWtem"),
					new MetricField("waterTemperatureMax", "maxWtem"),
					new MetricField("waveHeightMin", "minWvhgt"),
					new MetricField("waveHeightMax", "maxWvhgt"),
					new MetricField("airTemperatureMin", "minArtmp"),
					new MetricField("airTemperatureMax", "maxArtmp"),
					new MetricField("currentSpeedMin", "minCrsp"),
					new MetricField("currentSpeedMax", "maxCrsp"),
					new MetricField("windSpeedMin", "minWspd"),
					new MetricField("windSpeedMax", "maxWspd")
				),
				"seafsTgfshNm"
			);
		}

		static ApiSpec mudflat() {
			return new ApiSpec(Experience.MUDFLAT, "fcstMudflatv2/GetFcstMudflatApiServicev2", "mdftExpcnVlgNm",
				new org.springframework.util.LinkedMultiValueMap<>(),
				List.of(
					new MetricField("weather", "weather"),
					new MetricField("availableStartTime", "mdftExprnBgngTm"),
					new MetricField("availableEndTime", "mdftExprnEndTm"),
					new MetricField("airTemperatureMin", "minArtmp"),
					new MetricField("airTemperatureMax", "maxArtmp"),
					new MetricField("windSpeedMin", "minWspd"),
					new MetricField("windSpeedMax", "maxWspd")
				),
				null
			);
		}

		static ApiSpec scuba() {
			return new ApiSpec(Experience.SCUBA, "fcstSkinScubav2/GetFcstSkinScubaApiServicev2", "skscExpcnRgnNm",
				new org.springframework.util.LinkedMultiValueMap<>(),
				List.of(
					new MetricField("tideStage", "tdlvHrCn"),
					new MetricField("waterTemperatureMin", "minWtem"),
					new MetricField("waterTemperatureMax", "maxWtem"),
					new MetricField("waveHeightMin", "minWvhgt"),
					new MetricField("waveHeightMax", "maxWvhgt"),
					new MetricField("currentSpeedMin", "minCrsp"),
					new MetricField("currentSpeedMax", "maxCrsp")
				),
				null
			);
		}

		static ApiSpec surfing() {
			return new ApiSpec(Experience.SURFING, "fcstSurfingv2/GetFcstSurfingApiServicev2", "surfPlcNm",
				new org.springframework.util.LinkedMultiValueMap<>(),
				List.of(
					new MetricField("level", "grdCn"),
					new MetricField("waveHeight", "avgWvhgt"),
					new MetricField("wavePeriod", "avgWvpd"),
					new MetricField("windSpeed", "avgWspd"),
					new MetricField("waterTemperature", "avgWtem")
				),
				null
			);
		}

		String placeCode(JsonNode item) {
			return experience.apiValue() + ":" + item.path(nameField).asText();
		}

		String variantKey(JsonNode item) {
			return variantField == null ? "" : item.path(variantField).asText("");
		}

		ObjectNode metrics(JsonNode item) {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode node = mapper.createObjectNode();
			for (MetricField metric : metrics) {
				JsonNode value = item.path(metric.sourceField());
				if (!value.isMissingNode() && !value.isNull()) {
					node.set(metric.apiField(), value);
				}
			}
			return node;
		}
	}

	private record MetricField(String apiField, String sourceField) {
	}
}
