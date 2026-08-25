package com.marvinformatics.shard4j.coordinator.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Set;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * The shared secret is the only trust boundary; a session id is not a capability. Every
 * call is authenticated, including the read surface unless a deployment explicitly opts
 * into public reads because its session ids never appear in public logs.
 *
 * <p>Comparison is constant-time against every accepted value -- never a short-circuiting
 * equals on a secret compared on every request -- and the secret is never logged, echoed
 * or reflected into a response.
 */
public final class SecretAuthFilter extends OncePerRequestFilter {

  private static final String BEARER_PREFIX = "Bearer ";

  private final List<byte[]> acceptedSecrets;
  private final boolean publicRead;

  public SecretAuthFilter(Set<String> secrets, boolean publicRead) {
    this.acceptedSecrets =
        secrets.stream().map(secret -> secret.getBytes(StandardCharsets.UTF_8)).toList();
    this.publicRead = publicRead;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    if (permitted(request)) {
      chain.doFilter(request, response);
      return;
    }
    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    response.setContentType("application/json");
    response.getWriter().write("{\"error\":\"unauthorized\"}");
  }

  private boolean permitted(HttpServletRequest request) {
    String path = request.getRequestURI();
    if ("/healthz".equals(path) || "/readyz".equals(path)) {
      return true;
    }
    if (publicRead && "GET".equals(request.getMethod()) && path.startsWith("/sessions/")) {
      return true;
    }
    String header = request.getHeader("Authorization");
    if (header == null || !header.startsWith(BEARER_PREFIX)) {
      return false;
    }
    byte[] presented = header.substring(BEARER_PREFIX.length()).getBytes(StandardCharsets.UTF_8);
    boolean accepted = false;
    for (byte[] secret : acceptedSecrets) {
      accepted |= MessageDigest.isEqual(secret, presented);
    }
    return accepted;
  }
}
