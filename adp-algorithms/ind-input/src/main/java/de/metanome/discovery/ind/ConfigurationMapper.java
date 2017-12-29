package de.metanome.discovery.ind;

import com.google.common.base.Splitter;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.commons.beanutils.BeanUtils;

class ConfigurationMapper {

  static void applyFrom(final String value, final Object object) {
    if (value == null) {
      return;
    }

    final Map<String, String> kv = Splitter.on(",").trimResults().omitEmptyStrings()
        .withKeyValueSeparator("=").split(value);

    try {
      BeanUtils.populate(object, kv);
    } catch (final IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
