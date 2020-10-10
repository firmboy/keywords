package com.playboy.keywords;

import com.google.common.collect.Sets;
import com.playboy.util.ObjectUtils;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;

@Intercepts(
        {
                @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
                @Signature(type = Executor.class, method = "query", args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class, CacheKey.class, BoundSql.class}),
        }
)
public class KeyWordsPlugin implements Interceptor {
    private Logger log = LoggerFactory.getLogger(KeyWordsPlugin.class);

    private boolean LOAD;//文件是否加载成功
    private String KEY_WORDS = "";
    private Set<String> KEYS = Sets.newHashSet();
    private boolean ENABLE = false;
    private Field additionalParametersField;

    private static OracleSqlParser oracleSqlParser;


    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Object[] args = invocation.getArgs();
        MappedStatement ms = (MappedStatement) args[0];
        ms.getConfiguration();
        Object parameter = args[1];
        RowBounds rowBounds = (RowBounds) args[2];
        ResultHandler resultHandler = (ResultHandler) args[3];
        Executor executor = (Executor) invocation.getTarget();
        CacheKey cacheKey;
        BoundSql boundSql;
        //由于逻辑关系，只会进入一次
        if (args.length == 4) {
            //4 个参数时
            boundSql = ms.getBoundSql(parameter);
            cacheKey = executor.createCacheKey(ms, parameter, rowBounds, boundSql);
        } else {
            //6 个参数时
            cacheKey = (CacheKey) args[4];
            boundSql = (BoundSql) args[5];
        }
        if (ENABLE) {
            String sql = boundSql.getSql();

            boolean containFlag = false;
            for (String key : KEYS) {
                if (sql.contains(key)) {
                    containFlag = true;
                    break;
                }
            }
            if (containFlag) {
                log.info("sql含关键字");
                sql = oracleSqlParser.dealSql(sql);
                Map<String, Object> additionalParameters = (Map<String, Object>) additionalParametersField.get(boundSql);
                boundSql = new BoundSql(ms.getConfiguration(), sql, boundSql.getParameterMappings(), boundSql.getParameterObject());
                for (String key : additionalParameters.keySet()) {
                    boundSql.setAdditionalParameter(key, additionalParameters.get(key));
                }
            }
        }
        List resultList = executor.query(ms, parameter, rowBounds, resultHandler, cacheKey, boundSql);
        return resultList;
    }

    @Override
    public Object plugin(Object target) {
        return Plugin.wrap(target, this);
    }

    @Override
    public void setProperties(Properties prop) {
        if (!ObjectUtils.isEmpty(prop.getProperty("keywords"))) {
            KEY_WORDS = prop.getProperty("keywords");
            log.info("设置的关键字：" + KEY_WORDS);
            String[] split = KEY_WORDS.split(",");
            List<String> strings = Arrays.asList(split);
            KEYS = Sets.newHashSet(strings);
            oracleSqlParser = new OracleSqlParser(KEY_WORDS);
        }
        if (!ObjectUtils.isEmpty(prop.getProperty("enable"))) {
            String enable = prop.getProperty("enable");
            if (enable.equals("1")) {
                log.info("过滤关键字插件enable");
                ENABLE = true;
            }
        }
        try {
            //反射获取 BoundSql 中的 additionalParameters 属性
            additionalParametersField = BoundSql.class.getDeclaredField("additionalParameters");
            additionalParametersField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }

    }


}

