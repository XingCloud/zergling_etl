package com.elex.bigdata.zergling.etl.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * User: Z J Wu Date: 14-2-26 Time: 上午10:35 Package: com.elex.bigdata.zergling.etl.model
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface BigDataColumn {
  String cf();

  String q();

  HbaseDataType type() default HbaseDataType.STR;
}
