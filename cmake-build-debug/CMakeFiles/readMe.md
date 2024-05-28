Baza danych

Projekt implementuje język zapytań podobny do istniejącego języka SQL

W języku dostępne są polecenia:
CREATE
INSERT
UPDATE
SELECT
oraz klauzula
WHERE, której użycie jest możliwe w poleceniu SELECT oraz UPDATE.

Dostępne są następujące typy danych:
String
Integer
Float

Wprowadzanie poleceń:
Kolejne słowa polecenia należy oddzielać spacjami. Nie należy stosować znaków interpunkcyjnych. Nie należy również kończyć komendy znakiem “;”.

CREATE: pozwala na utworzenie nowej tabeli zawierającej wskazane kolumny.
Przykładowe użycie:
CREATE Student imię nazwisko wiek

INSERT: pozwala na wprowadzenie rekordu do istniejącej tablicy.
Przykładowe użycie:
INSERT imię Piotr nazwisko Kowalski wiek 22 INTO Student
Należy wprowadzać najpierw nazwę kolumny, następnie wartość. Nie należy brać wartości w cudzysłów.
Niepoprawne użycie:
INSERT imię “Piotr” nazwisko “Kowalski” wiek “22”
W przeciwieństwie do do klasycznego języka SQL, słowo kluczowe INTO oraz nazwę tabeli należy umieścić na końcu polecenia.
Niepoprawne użycie:
INSERT INTO table1 imię Piotr nazwisko Kowalski wiek 22

UPDATE

W komendzie UPDATE należy wskazać najpierw kolumny i wartości, następnie po słowie kluczowym IN nazwę tabeli, a na koniec po słowie kluczowym WHERE warunki. Szczegóły tworzenia warunków zostały opisane w podpunkcie SELECT.
UPDATE imie Andrzej IN table1 WHERE imie = Jacek


SELECT
Należy podać najpierw kolumnę, następnie warunek oznaczony klauzulą WHERE. Warunki można łączyć za pomocą słów kluczowych AND oraz OR. Nie należy używać wielokrotnie słowa WHERE. Tabelę należy określić za pomocą słowa FROM.
Można wykorzystywać następujące operatory:
=
!=
<
>
Przykładowe użycie:
SELECT imie FROM table1 WHERE imie = Andrzej AND nazwisko = Nowak
