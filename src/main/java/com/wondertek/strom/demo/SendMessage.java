package com.wondertek.strom.demo;

import java.util.Random;

public class SendMessage {
    Random r= new Random();
     public String  producerMsg(){
         for (int i=0;i<=1000;i++)
         {
    int id= r.nextInt(10000);
    int member=r.nextInt(100);
    double totalPrice=(double)r.nextInt(1000)/2;
    double reducer=(double)r.nextInt(10)/2;



         }
         return  "";
     }
}
