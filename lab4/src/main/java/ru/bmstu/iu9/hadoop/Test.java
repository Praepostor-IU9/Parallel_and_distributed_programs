package ru.bmstu.iu9.hadoop;

class Test {
    final String name;
    final Boolean res;
    final String descr;

    Test(String name, Boolean res, String descr) {
        this.name = name;
        this.res = res;
        this.descr = descr;
    }

    public String getName() {
        return name;
    }

    public Boolean getRes() {
        return res;
    }

    public String getDescr() {
        return descr;
    }

    @Override
    public String toString() {
        return "Test{" +
                "nameTest='" + name + '\'' +
                ", res=" + res +
                ", description='" + descr + '\'' +
                "}\n";
    }
}
