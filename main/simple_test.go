package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func readCSV(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	content, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return content, nil
}

func areEqualCSVs(path1 string, path2 string) (bool, error) {
	content1, err := readCSV(path1)
	if err != nil {
		return false, err
	}
	content2, err := readCSV(path2)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(content1, content2) {
		return false, fmt.Errorf("Expected output %+v, but got %+v", content1, content2)
	}
	return true, nil
}

func TestFullPipeline(t *testing.T) {
	err := Read("test_input.csv").
		With(GetRows(1, 4)).
		With(GetColumns(0, 2)).
		Write("actual_test_output_1.csv")

	if err != nil {
		t.Errorf(err.Error())
	}

	areEqual, err := areEqualCSVs("expected_test_output_1.csv", "actual_test_output_1.csv")
	if !areEqual {
		t.Errorf(err.Error())
	}

	err = Read("test_input.csv").
		With(GetRows(1, 20)).
		With(TopN(3, 2)).
		Write("actual_test_output_2.csv")

	if err != nil {
		t.Errorf(err.Error())
	}

	areEqual, err = areEqualCSVs("expected_test_output_2.csv", "actual_test_output_2.csv")
	if !areEqual {
		t.Errorf(err.Error())
	}

	err = Read("test_input.csv").
		With(GetRows(1, 20)).
		With(GetColumns(2, 3)).
		With(GetAvg()).
		Write("actual_test_output_3.csv")

	if err != nil {
		t.Errorf(err.Error())
	}

	areEqual, err = areEqualCSVs("expected_test_output_3.csv", "actual_test_output_3.csv")
	if !areEqual {
		t.Errorf(err.Error())
	}

	err = Read("test_input.csv").
		With(GetRows(1, 20)).
		With(GetColumns(2, 3)).
		With(GetAvg()).
		With(Ceil()).
		Write("actual_test_output_4.csv")

	if err != nil {
		t.Errorf(err.Error())
	}

	areEqual, err = areEqualCSVs("expected_test_output_4.csv", "actual_test_output_4.csv")
	if !areEqual {
		t.Errorf(err.Error())
	}
}
