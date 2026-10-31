package dev.dmitry.retailstream.projection;

import dev.dmitry.retailstream.contract.RawIdentity;
import java.util.Map;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

public final class SchemaManager {
  private final DataSource dataSource;
  private final JdbcTemplate jdbc;

  public SchemaManager(DataSource dataSource) {
    this.dataSource = dataSource;
    this.jdbc = new JdbcTemplate(dataSource);
  }

  public void createFresh(ProjectionConfig config, Map<String, Long> seed) {
    String digest =
        RawIdentity.digest(null, new JsonMapper().writeValueAsBytes(new java.util.TreeMap<>(seed)));
    if (seed.size() != 3 || !digest.equals(config.seedSha256()))
      throw new IllegalArgumentException("Seed differs from frozen metadata");
    if (jdbc.queryForObject(
            "select count(*) from information_schema.schemata where schema_name=?",
            Integer.class,
            config.schema())
        != 0) throw new IllegalArgumentException("Fresh schema already exists");
    jdbc.execute("create schema \"" + config.schema() + "\"");
    Flyway.configure()
        .dataSource(dataSource)
        .schemas(config.schema())
        .defaultSchema(config.schema())
        .createSchemas(false)
        .cleanDisabled(true)
        .load()
        .migrate();
    new TransactionTemplate(new JdbcTransactionManager(dataSource))
        .executeWithoutResult(
            status -> {
              jdbc.update(
                  "insert into " + config.table("projection_meta") + " values (1,?,?,?,?,1)",
                  config.topic(),
                  config.groupId(),
                  config.fixtureSha256(),
                  config.seedSha256());
              new java.util.TreeMap<>(seed)
                  .forEach(
                      (product, balance) ->
                          jdbc.update(
                              "insert into "
                                  + config.table("inventory")
                                  + "(product_id,balance) values (?,?)",
                              product,
                              balance));
            });
  }

  public void requireMatching(ProjectionConfig config) {
    var rows = jdbc.queryForList("select * from " + config.table("projection_meta"));
    if (rows.size() != 1) throw new IllegalArgumentException("Missing projection metadata");
    var row = rows.getFirst();
    if (!config.topic().equals(row.get("topic"))
        || !config.groupId().equals(row.get("group_id"))
        || !config.fixtureSha256().equals(row.get("fixture_sha256"))
        || !config.seedSha256().equals(row.get("seed_sha256"))
        || !Integer.valueOf(1).equals(row.get("seed_version")))
      throw new IllegalArgumentException("Projection source/group/fixture/seed mismatch");
  }
}
