package loghub;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestConvertGlobToRegex {

    @BeforeClass
    static public void configure() {
        Tools.configure();
    }

    @Test
    public void star_becomes_dot_star() {
        Assert.assertEquals("gl.*b", Helpers.convertGlobToRegex("gl*b").pattern());
    }

    @Test
    public void escaped_star_is_unchanged() {
        Assert.assertEquals("gl\\*b", Helpers.convertGlobToRegex("gl\\*b").pattern());
    }

    @Test
    public void question_mark_becomes_dot() {
        Assert.assertEquals("gl.b", Helpers.convertGlobToRegex("gl?b").pattern());
    }

    @Test
    public void escaped_question_mark_is_unchanged() {
        Assert.assertEquals("gl\\?b", Helpers.convertGlobToRegex("gl\\?b").pattern());
    }

    @Test
    public void character_classes_dont_need_conversion() {
        Assert.assertEquals("gl[-o]b", Helpers.convertGlobToRegex("gl[-o]b").pattern());
    }

    @Test
    public void escaped_classes_are_unchanged() {
        Assert.assertEquals("gl\\[-o\\]b", Helpers.convertGlobToRegex("gl\\[-o\\]b").pattern());
    }

    @Test
    public void negation_in_character_classes() {
        Assert.assertEquals("gl[^a-n!p-z]b", Helpers.convertGlobToRegex("gl[!a-n!p-z]b").pattern());
    }

    @Test
    public void nested_negation_in_character_classes() {
        Assert.assertEquals("gl[[^a-n]!p-z]b", Helpers.convertGlobToRegex("gl[[!a-n]!p-z]b").pattern());
    }

    @Test
    public void escape_carat_if_it_is_the_first_char_in_a_character_class() {
        Assert.assertEquals("gl[\\^o]b", Helpers.convertGlobToRegex("gl[^o]b").pattern());
    }

    @Test
    public void metachars_are_escaped() {
        Assert.assertEquals("gl..*\\.\\(\\)\\+\\|\\^\\$\\@\\%b", Helpers.convertGlobToRegex("gl?*.()+|^$@%b").pattern());
    }

    @Test
    public void metachars_in_character_classes_dont_need_escaping() {
        Assert.assertEquals("gl[?*.()+|^$@%]b", Helpers.convertGlobToRegex("gl[?*.()+|^$@%]b").pattern());
    }

    @Test
    public void escaped_backslash_is_unchanged() {
        Assert.assertEquals("gl\\\\b", Helpers.convertGlobToRegex("gl\\\\b").pattern());
    }

    @Test
    public void slashQ_and_slashE_are_escaped() {
        Assert.assertEquals("\\\\Qglob\\\\E", Helpers.convertGlobToRegex("\\Qglob\\E").pattern());
    }

    @Test
    public void braces_are_turned_into_groups() {
        Assert.assertEquals("(glob|regex)", Helpers.convertGlobToRegex("{glob,regex}").pattern());
    }

    @Test
    public void escaped_braces_are_unchanged() {
        Assert.assertEquals("\\{glob\\}", Helpers.convertGlobToRegex("\\{glob\\}").pattern());
    }

    @Test
    public void commas_dont_need_escaping() {
        Assert.assertEquals("(glob,regex),", Helpers.convertGlobToRegex("{glob\\,regex},").pattern());
    }

}
