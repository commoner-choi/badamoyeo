package badamoyeo_api.ingestion.fetch;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public record FetchPage(List<JsonNode> items, int totalCount) {
}
