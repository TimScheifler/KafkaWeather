package org.example.service;

import org.example.producer.KafkaWeatherProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import static org.example.utils.WeatherUtils.LATITUDE_WETTINGEN;
import static org.example.utils.WeatherUtils.LONGITUDE_WETTINGEN;
import static org.example.utils.WeatherUtils.OPEN_METEO_URL;

@Service
public class WeatherDataService {

    private static final Logger LOG = LoggerFactory.getLogger(WeatherDataService.class);
    private static final KafkaWeatherProducer weatherProducer = new KafkaWeatherProducer();

    @Scheduled(fixedRate = 900000)
    public void fetchWeatherData() {
        String url = UriComponentsBuilder.fromHttpUrl(OPEN_METEO_URL)
                .queryParam("latitude", LATITUDE_WETTINGEN)
                .queryParam("longitude", LONGITUDE_WETTINGEN)
                .queryParam("hourly", "temperature_2m", "rain", "snowfall", "cloud_cover", "wind_speed_10m", "wind_direction_10m")
                .queryParam("timezone", "Europe/Berlin")
                .queryParam("forecast_days", "3")
                .toUriString();
        RestTemplate restTemplate = new RestTemplate();
        String response = restTemplate.getForObject(url, String.class);
        LOG.info(response);
        weatherProducer.prouce("weather", LATITUDE_WETTINGEN + "_" + LONGITUDE_WETTINGEN, response);
    }
}
