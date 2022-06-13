package com.mozilla.telemetry;

import com.google.zetasql.AnalyzerOptions;
import com.google.zetasql.Analyzer;
import com.google.zetasql.LanguageOptions;
import com.google.zetasql.ZetaSQLOptions.LanguageFeature;
import java.util.List;

/**
 * Provide a helper to work around pyjnius being unable to import {@link Analyzer}.
 */
public class ZetaSqlHelper {
  private ZetaSqlHelper() {
  }

  private static AnalyzerOptions options = new AnalyzerOptions();
  private static LanguageOptions languageOptions = options.getLanguageOptions();

  static {
    languageOptions.setSupportsAllStatementKinds();
    languageOptions.enableMaximumLanguageFeatures();
    languageOptions.enableLanguageFeature(LanguageFeature.FEATURE_V_1_3_QUALIFY);
  }

  static List<List<String>> extractTableNamesFromStatement(String sql) {
    return Analyzer.extractTableNamesFromStatement(sql, options);
  }

  static List<List<String>> extractTableNamesFromScript(String sql) {
    return Analyzer.extractTableNamesFromScript(sql, options);
  }
}
