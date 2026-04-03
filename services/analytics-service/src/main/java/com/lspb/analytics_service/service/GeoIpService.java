package com.lspb.analytics_service.service;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Subdivision;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;

@Slf4j
@Service
public class GeoIpService {

    private DatabaseReader dbReader;

    @PostConstruct
    public void init() {
        try {
            Resource resource = new ClassPathResource("GeoLite2-City.mmdb");
            if (resource.exists()) {
                InputStream dbAsStream = resource.getInputStream();
                dbReader = new DatabaseReader.Builder(dbAsStream).build();
                log.info("Successfully loaded MaxMind GeoIP database from classpath.");
            } else {
                log.warn("GeoLite2-City.mmdb not found in classpath. GeoIP enrichment will be disabled.");
            }
        } catch (Exception e) {
            log.warn("Failed to initialize GeoIP database reader. Reason: {}", e.getMessage());
        }
    }

    public GeoData getGeoData(String ipAddress) {
        if (dbReader == null || ipAddress == null || ipAddress.isEmpty()) {
            return new GeoData(null, null);
        }

        try {
            InetAddress ipAddressObj = InetAddress.getByName(ipAddress);
            CityResponse response = dbReader.city(ipAddressObj);

            Country country = response.getCountry();
            String countryIsoCode = country != null ? country.getIsoCode() : null;

            Subdivision subdivision = response.getMostSpecificSubdivision();
            String regionName = subdivision != null ? subdivision.getName() : null;

            return new GeoData(countryIsoCode, regionName);
        } catch (Exception e) {
            log.debug("GeoIP lookup failed for IP {}: {}", ipAddress, e.getMessage());
            return new GeoData(null, null);
        }
    }

    public record GeoData(String countryCode, String region) {}
}
