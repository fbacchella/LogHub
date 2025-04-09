package loghub.commands;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;

import lombok.Getter;

@Getter
public class BaseParameters {

    @Parameter(names = {"--help", "-h"}, help = true)
    private boolean help = false;

    @Parameter(description = "Main parameters")
    private List<String> mainParams = new ArrayList<>();

}
