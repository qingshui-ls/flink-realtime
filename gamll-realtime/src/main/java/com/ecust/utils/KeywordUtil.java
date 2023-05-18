package com.ecust.utils;


import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyword(String word) throws IOException {
        ArrayList<String> arrayList = new ArrayList<>();
        // 利用ik分词器分词
        // 1.创建IK分词对象
        StringReader reader = new StringReader(word);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);
        // 2.取出结果
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            String text = next.getLexemeText();
            arrayList.add(text);
            next = ikSegmenter.next();
        }
        return arrayList;
    }

}
