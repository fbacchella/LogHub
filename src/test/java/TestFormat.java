import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import loghub.VarFormatter;

public class TestFormat {
    
    static Map<String,Set<String>> symbolsSet = new HashMap<>();

    static public void main(final String[] args) {
        
        Object val = (int)(Math.PI * 1e6);
        String format = "%#X";
        Map<String, Object> values = new HashMap<>();
        values.put("truc", "a");
        values.put("machin", 255);
        values.put("bidule", "c");
        values.put("var", "d");
        values.put("pi", val);

//        Date start = new Date();
//        Date end = new Date();
//        
//        start = new Date();
//        for(int i = 0 ; i < 1000000 ; i++) {
//            String a = String.format("% 10.2f",  Math.PI * 1e6);
//        }
//        System.out.println(new Date().getTime() - start.getTime());
//        
//        start = new Date();
//        VarFormatter vf0 = new VarFormatter("${pi% 10.2f}", Locale.getDefault());
//        for(int i = 0 ; i < 1000000 ; i++) {
//            String a = vf0.format(values);
//        }
//        System.out.println(new Date().getTime() - start.getTime());

        //System.out.println(String.format("% 10.2f",  Math.PI * 1e6));
        Arrays.stream(new String[] {"Currency", "CurrencySymbol", "DecimalSeparator", "Digit", "ExponentSeparator", "GroupingSeparator", "Infinity",
                "InternationalCurrencySymbol", "MinusSign", "MonetaryDecimalSeparator", "NaN", "PatternSeparator", "Percent", "PerMill", "ZeroDigit"
                }).forEach( i-> symbolsSet.put(i, new HashSet<>()));
        for(Locale l: Locale.getAvailableLocales()) {
//            System.out.println(l.getDisplayName());
//            System.out.println("    " + String.format(l, format, val));
//            VarFormatter vf = new VarFormatter("${pi" + format + "}", l);
//            System.out.println("    " + vf.format(values));
            dumpSymbols(l);
        }
        symbolsSet.forEach( (i, j) -> {
            System.out.println(i);
            System.out.print("    ");
            System.out.println(j.stream()
            .map(k -> "'" + k + "'")
            .collect(Collectors.joining(" ")));

        });
//        System.out.println(new DecimalFormat("#").format(Math.PI * 1e6));
//        System.out.println(new DecimalFormat("#.###").format(12345.0123));
//        System.out.println(new DecimalFormat("###E0.###").format(123456789 + 0.9876543210));

        //VarFormatter vf = new VarFormatter("At ${truc%u} on ${machin%H}, there was ${truc} on planet ${bidule%S} ${var%0.2d}.", Locale.getDefault());
        //System.out.println(vf.format(values));
    }

    static private final void dumpSymbols(Locale l) {
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(l);
        System.out.println(l.getDisplayName());
        System.out.println("    Currency:" + symbols.getCurrency()); symbolsSet.get("Currency").add(symbols.getCurrency().toString());
        System.out.println("    CurrencySymbol:" + symbols.getCurrencySymbol()); symbolsSet.get("CurrencySymbol").add(symbols.getCurrencySymbol());
        System.out.println("    DecimalSeparator:" + symbols.getDecimalSeparator()); symbolsSet.get("DecimalSeparator").add("" + symbols.getDecimalSeparator());
        System.out.println("    Digit:" + symbols.getDigit()); symbolsSet.get("Digit").add("" + symbols.getDigit());
        System.out.println("    ExponentSeparator:" + symbols.getExponentSeparator()); symbolsSet.get("ExponentSeparator").add(symbols.getExponentSeparator());
        System.out.println("    GroupingSeparator:" + symbols.getGroupingSeparator()); symbolsSet.get("GroupingSeparator").add("" + symbols.getGroupingSeparator());
        System.out.println("    Infinity:" + symbols.getInfinity()); symbolsSet.get("Infinity").add("" + symbols.getInfinity());
        System.out.println("    InternationalCurrencySymbol:" + symbols.getInternationalCurrencySymbol()); symbolsSet.get("InternationalCurrencySymbol").add(symbols.getInternationalCurrencySymbol());
        System.out.println("    MinusSign:" + symbols.getMinusSign()); symbolsSet.get("MinusSign").add("" + symbols.getMinusSign());
        System.out.println("    MonetaryDecimalSeparator:" + symbols.getMonetaryDecimalSeparator()); symbolsSet.get("MonetaryDecimalSeparator").add("" + symbols.getMonetaryDecimalSeparator());
        System.out.println("    NaN:" + symbols.getNaN()); symbolsSet.get("NaN").add(symbols.getNaN());
        System.out.println("    PatternSeparator:" + symbols.getPatternSeparator()); symbolsSet.get("PatternSeparator").add("" + symbols.getPatternSeparator());
        System.out.println("    Percent:" + symbols.getPercent()); symbolsSet.get("Percent").add("" + symbols.getPercent());
        System.out.println("    PerMill:" + symbols.getPerMill()); symbolsSet.get("PerMill").add("" + symbols.getPerMill());
        System.out.println("    ZeroDigit:" + symbols.getZeroDigit()); symbolsSet.get("ZeroDigit").add("" + symbols.getZeroDigit());
       
    }
}
