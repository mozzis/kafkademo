package com.example.kafkademo;

public class MResponse {
    private String mText;
    private int serial;
    private float sum;

    public MResponse() {
        // required by Jackson
    }

    public MResponse(String mText, int serial, float sum) {
        this.mText = mText;
        this.serial = serial;
        this.sum = sum;
    }

    public String getmText() { return mText; }
    public void setmText(String mText) { this.mText = mText; }

    public int getmSerial() { return serial; }
    public void setmSerial(int serial) { this.serial = serial; }

    public float getSum() { return sum; }
    public void setSum(float sum) { this.sum = sum; }
}
