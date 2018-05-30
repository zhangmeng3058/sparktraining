package org.china.meng.java;


/**
 * 冒泡排序
 */
public class BubbleSort {
    public static void bubbleSort(int[] a) {
        for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < a.length - i - 1; j++) {
                if (a[j] > a[j + 1]) {
                    int temp;
                    temp = a[j];
                    a[j] = a[j + 1];
                    a[j + 1] = temp;
                }
            }
        }
        for (int i = 0; i < a.length; i++) {
            System.out.print(a[i] + ",");
        }
    }

    public static void bubbleSort2(int[] a) {
        int j, k = a.length;
        boolean flag = true;
        while (flag) {
            flag = false;
            for (j = 1; j < k; j++) {
                if (a[j - 1] > a[j]) {
                    int temp;
                    temp = a[j - 1];
                    a[j - 1] = a[j];
                    a[j] = temp;

                    flag = true;
                }
            }
            k--;
        }
        for (int i = 0; i < a.length; i++) {
            System.out.print(a[i] + ",");
        }
    }


    public static void bubbleSort3(int[] a) {
        int j, k;
        int flag = a.length;

        while (flag > 0) {
            k = flag;
            flag = 0;
            for (j = 1; j < k; j++) {
                if (a[j - 1] > a[j]) {
                    int temp;
                    temp = a[j - 1];
                    a[j - 1] = a[j];
                    a[j] = temp;

                    flag = j;
                }
            }

        }
        for (int i = 0; i < a.length; i++) {
            System.out.print(a[i] + ",");
        }
    }

    public static void main(String[] args) {
        int[] arr = {24, 1, 89, 5, 33, 47, 4, 88, 43};
        bubbleSort3(arr);
    }

}
