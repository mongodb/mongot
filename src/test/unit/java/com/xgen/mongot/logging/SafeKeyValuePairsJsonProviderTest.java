package com.xgen.mongot.logging;

import static com.google.common.truth.Truth.assertThat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.helpers.NOPAppender;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import net.logstash.logback.composite.JsonProviders;
import net.logstash.logback.composite.loggingevent.LoggingEventCompositeJsonFormatter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.KeyValuePair;

public class SafeKeyValuePairsJsonProviderTest {

  private LoggingEventCompositeJsonFormatter formatter;
  private LoggingEvent event;
  private ByteArrayOutputStream resultStream;

  @Before
  public void setup() throws Exception {
    this.formatter = new LoggingEventCompositeJsonFormatter(new NOPAppender<>());
    var jsonProviders = new JsonProviders<ILoggingEvent>();
    jsonProviders.addProvider(new SafeKeyValuePairsJsonProvider());
    this.formatter.setProviders(jsonProviders);
    this.formatter.start();
    this.event = new LoggingEvent();
    this.resultStream = new ByteArrayOutputStream();
  }

  @Test
  public void testNullKeyOmitted() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair(null, "value"));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{}");
  }

  @Test
  public void testEmptyStringKeyOmitted() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("", "value"));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{}");
  }

  @Test
  public void testEmptyOptionalValueOmitted() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key", Optional.empty()));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{}");
  }

  @Test
  public void testOverrideExistingKeyValue() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key", "value1"));
    this.event.addKeyValuePair(new KeyValuePair("key", "value2"));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"key\":\"value2\"}");
  }

  @Test
  public void testNullValue() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key", null));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"key\":\"null\"}");
  }

  @Test
  public void testEmptyListValue() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key", Collections.emptyList()));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"key\":\"[]\"}");
  }

  @Test
  public void testStringValue() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key", "test"));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"key\":\"test\"}");
  }

  @Test
  public void testNumberValue() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key", 7));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"key\":7}");
  }

  @Test
  public void testBooleanValue() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key", true));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"key\":true}");
  }

  @Test
  public void testInstantValue() throws IOException {
    Instant timestamp = Instant.parse("1215-06-15T12:00:00.123Z");
    this.event.addKeyValuePair(new KeyValuePair("time", timestamp));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"time\":\"1215-06-15T12:00:00.123Z\"}");
  }

  @Test
  public void testMultipleKeyValuePairs() throws IOException {
    this.event.addKeyValuePair(new KeyValuePair("key1", "value1"));
    this.event.addKeyValuePair(new KeyValuePair("key2", "value2"));
    this.formatter.writeEvent(this.event, this.resultStream);
    assertThat(this.resultStream.toString()).isEqualTo("{\"key1\":\"value1\",\"key2\":\"value2\"}");
  }
}
