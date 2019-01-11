package ru.bmstu.iu9.hadoop;

import java.util.Arrays;

class Result {
    final Test[] result;

    Result(Test[] result) {
        this.result = result;
    }

    public Test[] getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "Result{\n" +
                Arrays.toString(result) +
                '}';
    }
}
