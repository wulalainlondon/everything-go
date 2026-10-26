package callbacks

import (
	"strings"
	"testing"
)

func TestExpectedIdentifierNumericReadingsPreserveValueAndEveryOtherWord(t *testing.T) {
	expected := "結果 J486 底線 S10 底線 CAMERA 底線 OK，完成 2 項，沒有刪除。"
	for _, actual := range []string{
		expected,
		"結果J四八六底線S十底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四百八十六底線S一零底線camera底線ok，完成2項，沒有刪除。",
	} {
		if matched, prefix := CompareSpeech(expected, actual, 2); !matched || prefix {
			t.Fatalf("exact same-value reading rejected: %q (%v,%v)", actual, matched, prefix)
		}
	}
	for _, actual := range []string{
		"結果J四八七底線S十底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四八六底線S十一底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四八六底線S一百底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四八底線S十底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四8六底線S十底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果K四八六底線S十底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四八六底線S十底線CAMERA底線OK，完成二項，沒有刪除。",
		"結果J四八六底線S十底線CAMERA底線OK，完成3項，沒有刪除。",
		"結果J四八六底線S十底線CAMERA底線OK，完成2項，已刪除。",
		"結果S十底線J四八六底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四八六S十底線CAMERA底線OK，完成2項，沒有刪除。",
		"結果J四八六底線S十底線CAMERA底線OK，完成2項，沒有刪除。其他任務完成",
	} {
		if matched, _ := CompareSpeech(expected, actual, 2); matched {
			t.Fatalf("different content accepted: %q", actual)
		}
	}
}

func TestIdentifierOnlyNotDecimalSignedPercentageEmbeddedOrLeadingZeroNormalization(t *testing.T) {
	for _, pair := range [][2]string{
		{"J1.5", "J一.五"}, {"V1.2.3", "V一.二.三"}, {"S10%", "S十%"}, {"-S10", "-S十"}, {"S10-20", "S十-20"}, {"A10/20", "A十/20"}, {"A10:20", "A十:20"},
		{"A10B", "A十B"}, {"1A10", "1A十"}, {"S010", "S十"}, {"S010", "S一零"}, {"J486", "J四百八十七"}, {"S10", "S一百"}, {"S10", "S兩五"}, {"Ｓ10", "Ｓ十"},
		{"完成10項", "完成十項"}, {"數值1.5", "數值15"}, {"沒有刪除", "已刪除"},
	} {
		if match, _ := CompareSpeech(pair[0], pair[1], 2); match {
			t.Fatalf("numeric meaning broadened: %q => %q", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{{"S010", "S零一零"}, {"J0", "J零"}, {"A1010", "A一千零一十"}, {"A110", "A一百一十"}, {"A1001", "A一千零一"}, {"A9999", "A九千九百九十九"}} {
		if match, _ := CompareSpeech(pair[0], pair[1], 2); !match {
			t.Fatalf("expected exact integer pronunciation rejected: %v", pair)
		}
	}
}

func TestNumericAlternativePrefixesWaitForTheEntireFrozenSpeech(t *testing.T) {
	expected := "結果J486底線S10底線OK，沒有刪除。"
	for _, actual := range []string{"", "結果J四", "結果J四八", "結果J四八六底線S一", "結果J四百八十", "結果J四八六底線S十底線OK"} {
		if match, prefix := CompareSpeech(expected, actual, 2); match || !prefix {
			t.Fatalf("partial transcript settled: %q (%v,%v)", actual, match, prefix)
		}
	}
	if match, prefix := CompareSpeech(expected, "結果J四八七", 2); match || prefix {
		t.Fatal("wrong digit treated as a valid prefix")
	}
	if match, _ := CompareSpeech(expected, "結果J四八六底線S十底線OK，沒有刪除。結果J四八六", 2); match {
		t.Fatal("duplicate segment accepted")
	}
}

func TestLegacySpeechAndFiniteComparisonBoundsRemainConservative(t *testing.T) {
	for _, version := range []int{0, 1, 3} {
		if match, _ := CompareSpeech("J486底線S10", "J四八六底線S十", version); match {
			t.Fatal("legacy/unsupported version upgraded")
		}
		if match, _ := CompareSpeech("J486底線S10", "j486 底線 s10", version); !match {
			t.Fatal("existing strict behavior changed")
		}
	}
	if match, _ := CompareSpeech(ReadableSpeech("", "result_ready", "J485_S10_TEXT_OK"), "J485S10TEXTOK", 2); match {
		t.Fatal("original485 lost underscore now falsely verified")
	}
	if match, prefix := CompareSpeech(strings.Repeat("x", 24001), "x", 2); match || prefix {
		t.Fatal("byte bound lost")
	}
	expected := strings.Repeat("A10 ", 65)
	if match, _ := CompareSpeech(expected, strings.Repeat("A十 ", 65), 2); match {
		t.Fatal("identifier bound expanded")
	}
	if match, _ := CompareSpeech(expected, expected, 2); !match {
		t.Fatal("strict equal bounded fallback lost")
	}
	if match, _ := CompareSpeech("A1234567", "A一二三四五六七", 2); match {
		t.Fatal("numeric digit bound expanded")
	}
}
