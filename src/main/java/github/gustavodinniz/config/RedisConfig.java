package github.gustavodinniz.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import github.gustavodinniz.model.StopOrder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(ReactiveRedisConnectionFactory connectionFactory, ObjectMapper objectMapper) {
        StringRedisSerializer keySerializer = new StringRedisSerializer();

        ObjectMapper customObjectMapper = objectMapper.copy();
        customObjectMapper.registerModule(new JavaTimeModule());

        customObjectMapper.activateDefaultTyping(
                BasicPolymorphicTypeValidator.builder()
                        .allowIfBaseType(Object.class)
                        .build(),
                ObjectMapper.DefaultTyping.NON_FINAL
        );

        Jackson2JsonRedisSerializer<Object> valueSerializer =
                new Jackson2JsonRedisSerializer<>(customObjectMapper, Object.class);


        RedisSerializationContext.RedisSerializationContextBuilder<String, Object> builder =
                RedisSerializationContext.newSerializationContext(keySerializer);

        RedisSerializationContext<String, Object> context = builder.value(valueSerializer)
                .hashKey(keySerializer)
                .hashValue(valueSerializer)
                .build();

        return new ReactiveRedisTemplate<>(connectionFactory, context);
    }

    @Bean
    public ReactiveRedisTemplate<String, StopOrder> reactiveStopOrderRedisTemplate(
            ReactiveRedisConnectionFactory connectionFactory, ObjectMapper objectMapper) {

        StringRedisSerializer keySerializer = new StringRedisSerializer();

        ObjectMapper customObjectMapper = objectMapper.copy();
        customObjectMapper.registerModule(new JavaTimeModule());

        Jackson2JsonRedisSerializer<StopOrder> valueSerializer =
                new Jackson2JsonRedisSerializer<>(customObjectMapper, StopOrder.class);

        RedisSerializationContext.RedisSerializationContextBuilder<String, StopOrder> builder =
                RedisSerializationContext.newSerializationContext(keySerializer);

        RedisSerializationContext<String, StopOrder> context = builder.value(valueSerializer).build();

        return new ReactiveRedisTemplate<>(connectionFactory, context);
    }
}
