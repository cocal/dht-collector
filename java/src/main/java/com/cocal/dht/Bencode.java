package com.cocal.dht;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class Bencode {
  private Bencode() {}

  static Object decode(byte[] input) {
    Parser parser = new Parser(input);
    Object value = parser.value();
    if (!parser.done()) throw new IllegalArgumentException("trailing bencode data");
    return value;
  }

  static byte[] encode(Object value) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    write(out, value);
    return out.toByteArray();
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> dict(Object value) {
    if (!(value instanceof Map<?, ?> map)) return Map.of();
    return (Map<String, Object>) map;
  }

  static String text(Object value) {
    return value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value);
  }

  static byte[] bytes(Object value) {
    if (value instanceof byte[] bytes) return bytes;
    return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
  }

  private static void write(ByteArrayOutputStream out, Object value) {
    if (value instanceof Map<?, ?> map) {
      out.write('d');
      map.entrySet().stream().sorted((a, b) -> String.valueOf(a.getKey()).compareTo(String.valueOf(b.getKey())))
          .forEach(entry -> { write(out, String.valueOf(entry.getKey()).getBytes(StandardCharsets.UTF_8)); write(out, entry.getValue()); });
      out.write('e');
    } else if (value instanceof List<?> list) {
      out.write('l'); list.forEach(item -> write(out, item)); out.write('e');
    } else if (value instanceof Number number) {
      writeAscii(out, "i" + number.longValue() + "e");
    } else {
      byte[] bytes = value instanceof byte[] raw ? raw : String.valueOf(value).getBytes(StandardCharsets.UTF_8);
      writeAscii(out, Integer.toString(bytes.length)); out.write(':'); out.writeBytes(bytes);
    }
  }

  private static void writeAscii(ByteArrayOutputStream out, String text) { out.writeBytes(text.getBytes(StandardCharsets.US_ASCII)); }

  private static final class Parser {
    private final byte[] input; private int index;
    Parser(byte[] input) { this.input = input; }
    boolean done() { return index == input.length; }
    Object value() {
      if (index >= input.length) throw new IllegalArgumentException("truncated bencode");
      return switch (input[index]) { case 'd' -> dictionary(); case 'l' -> list(); case 'i' -> integer(); default -> string(); };
    }
    private Map<String, Object> dictionary() {
      index++; Map<String, Object> result = new LinkedHashMap<>();
      while (index < input.length && input[index] != 'e') result.put(new String((byte[]) string(), StandardCharsets.UTF_8), value());
      if (index >= input.length) throw new IllegalArgumentException("unterminated dictionary"); index++; return result;
    }
    private List<Object> list() {
      index++; List<Object> result = new ArrayList<>();
      while (index < input.length && input[index] != 'e') result.add(value());
      if (index >= input.length) throw new IllegalArgumentException("unterminated list"); index++; return result;
    }
    private Long integer() {
      index++; int start = index; while (index < input.length && input[index] != 'e') index++;
      if (index >= input.length) throw new IllegalArgumentException("unterminated integer");
      long value = Long.parseLong(new String(input, start, index - start, StandardCharsets.US_ASCII)); index++; return value;
    }
    private byte[] string() {
      int start = index; while (index < input.length && input[index] != ':') {
        if (input[index] < '0' || input[index] > '9') throw new IllegalArgumentException("invalid byte string length"); index++;
      }
      if (index >= input.length) throw new IllegalArgumentException("truncated byte string");
      int length = Integer.parseInt(new String(input, start, index - start, StandardCharsets.US_ASCII)); index++;
      if (length < 0 || index + length > input.length) throw new IllegalArgumentException("truncated byte string data");
      byte[] value = java.util.Arrays.copyOfRange(input, index, index + length); index += length; return value;
    }
  }
}
