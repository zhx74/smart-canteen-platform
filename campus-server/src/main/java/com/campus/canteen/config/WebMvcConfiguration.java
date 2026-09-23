package com.campus.canteen.config;

import com.campus.canteen.interceptor.JwtTokenAdminInterceptor;
import com.campus.canteen.interceptor.JwtTokenUserInterceptor;
import com.campus.canteen.json.JacksonObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
@Slf4j
public class WebMvcConfiguration implements WebMvcConfigurer {

    @Autowired
    private JwtTokenAdminInterceptor jwtTokenAdminInterceptor;

    @Autowired
    private JwtTokenUserInterceptor jwtTokenUserInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        log.info("开始注册自定义拦截器...");
        registry.addInterceptor(jwtTokenAdminInterceptor)
                .addPathPatterns("/admin/**")
                .excludePathPatterns("/admin/employee/login");

        registry.addInterceptor(jwtTokenUserInterceptor)
                .addPathPatterns("/user/**")
                .addPathPatterns("/ai/**")
                .excludePathPatterns("/user/user/login")
                .excludePathPatterns("/user/user/login/phone")
                .excludePathPatterns("/user/user/status");
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/doc.html").addResourceLocations("classpath:/META-INF/resources/");
        registry.addResourceHandler("/webjars/**").addResourceLocations("classpath:/META-INF/resources/webjars/");
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true)
                .maxAge(3600);
    }

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        log.info("扩展消息转换器");
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter() {
            /**
             * ⚠️ 必须主动让出 byte[]，否则 Knife4j 文档页会报「Knife4j文档请求异常」。
             *
             * springdoc 2.x 的 /v3/api-docs（含 /v3/api-docs/{分组}）返回类型是 byte[]
             * 而不是 String；而本转换器被插到了 index 0，会抢在
             * ByteArrayHttpMessageConverter 之前接管响应，Jackson 序列化 byte[] 的
             * 默认行为又是**输出 Base64 字符串** → 整份 OpenAPI JSON 被变成
             * "eyJvcGVuYXBp..." 这种值 → Knife4j 前端 JSON.parse 拿到的是字符串而非
             * 对象，解析分组失败。
             *
             * 这里显式对 byte[] 返回 false，让它落回 ByteArrayHttpMessageConverter
             * 原样输出；其余类型（Result<T> 等）行为完全不变。
             */
            @Override
            public boolean canWrite(Class<?> clazz, org.springframework.http.MediaType mediaType) {
                return !byte[].class.equals(clazz) && super.canWrite(clazz, mediaType);
            }
        };
        converter.setObjectMapper(new JacksonObjectMapper());
        converters.add(0, converter);
    }
}
