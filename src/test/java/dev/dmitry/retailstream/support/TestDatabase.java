package dev.dmitry.retailstream.support;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.time.Duration;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

public final class TestDatabase implements AutoCloseable {
  public static final String IMAGE =
      "postgres@sha256:2cd2c0f809604aaeff6f0ec8f40aecbd025613107561015c69369a1a7f1d3416";
  public final PostgreSQLContainer container =
      new PostgreSQLContainer(IMAGE)
          .withStartupTimeout(Duration.ofSeconds(30))
          .withCreateContainerCmdModifier(
              cmd ->
                  cmd.getHostConfig()
                      .withMemory(512L * 1024 * 1024)
                      .withPortBindings(
                          new PortBinding(
                              Ports.Binding.bindIpAndPort("127.0.0.1", 0), ExposedPort.tcp(5432))));
  public DataSource dataSource;

  public TestDatabase() {
    container.start();
    dataSource =
        new DriverManagerDataSource(
            container.getJdbcUrl(), container.getUsername(), container.getPassword());
    if (new JdbcTemplate(dataSource).queryForObject("select 1", Integer.class) != 1)
      throw new IllegalStateException("Database probe failed");
    System.out.println("ACTUAL_DATABASE_IMAGE=" + container.getDockerImageName());
  }

  @Override
  public void close() {
    container.close();
  }
}
