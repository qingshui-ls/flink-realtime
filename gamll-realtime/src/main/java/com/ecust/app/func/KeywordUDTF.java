package com.ecust.app.func;

import com.ecust.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("Row<word String>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String s) {
        List<String> keywords = null;
        try {
            keywords = KeywordUtil.splitKeyword(s);
            keywords.forEach(x -> collect(Row.of(x)));
        } catch (IOException e) {
            collect(Row.of(s));
        }
    }
}
