package com.example.kafkademo;

public class MQuery {
    private String mText;
    private int serial;
    private float addend1;
    private float addend2;

    public MQuery() {
        // required by Jackson
    }

    private static int mSerial = 0;

    public MQuery(String mText, float addend1, float addend2) {
        this.mText = mText;
        this.serial = mSerial++;
        this.addend1 = addend1;
        this.addend2 = addend2;
    }

    public String getmText() { return mText; }
    public void setmText(String mText) { this.mText = mText; }

    public int getmSerial() { return this.serial; }
    public void setmSerial(int nValue) {this.serial = nValue; }

    public float getAddend1() { return addend1; }
    public void setAddend1(float addend1) { this.addend1 = addend1; }

    public float getAddend2() { return addend2; }
    public void setAddend2(float addend2) { this.addend2 = addend2; }
}
