package dev.dmitry.retailstream;

import dev.dmitry.retailstream.cli.StreamCommands;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;

@SpringBootConfiguration
public class StreamApplication {
  @Bean
  StreamCommands streamCommands() {
    return new StreamCommands();
  }

  public static void main(String[] args) {
    int result;
    try (var context =
        new SpringApplicationBuilder(StreamApplication.class)
            .web(WebApplicationType.NONE)
            .logStartupInfo(false)
            .run()) {
      result = context.getBean(StreamCommands.class).execute(args);
    }
    System.exit(result);
  }
}
