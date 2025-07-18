%macro calc_by_region(start=2002, end=2021, codelen=1);

   /* 0. mapping.xlsx 파일을 불러오기 (바탕화면 경로로 설정) */
   proc import datafile="C:\Users\YourUserName\Desktop\mapping.xlsx"
       out=work.mapping
       dbms=xlsx
       replace;
       sheet="Sheet1";  /* 시트명이 다르면 변경 */
       getnames=yes;
   run;

   %do year = &start %to &end;
      
      /* 1) 필요한 컬럼 추출 -> inc_연도 테이블 (예시) */
      PROC SQL;
         CREATE TABLE inc_&year AS
         SELECT
            SUBSTR(RVSN_ADDR_CD, 1, &codelen) AS region_code,
            INC_TOT
         FROM mylib.dses_&year
         WHERE INC_TOT > 0
         ORDER BY region_code, INC_TOT
         ;
      QUIT;

      /* 2) region_code별 지니계수 -> gini_연도 */
      DATA gini_&year (KEEP=region_code gini);
         SET inc_&year;
         BY region_code;
         RETAIN rank 0 n 0 sum_inc 0 partial 0;
         IF FIRST.region_code THEN DO;
            rank = 0; n = 0; sum_inc = 0; partial = 0;
         END;
         rank + 1;
         n + 1;
         sum_inc + INC_TOT;
         partial + (2 * rank) * INC_TOT;
         IF LAST.region_code THEN DO;
            gini = (partial - (n + 1)*sum_inc) / (n*sum_inc);
            OUTPUT;
         END;
      RUN;

      /* 3) 통계치 -> stats_연도 */
      PROC SQL;
         CREATE TABLE stats_&year AS
         SELECT 
            region_code,
            COUNT(*)          AS cnt,
            MEAN(INC_TOT)     AS avg_inc,
            MAX(INC_TOT)      AS max_inc,
            STD(INC_TOT)      AS std_inc,
            MEDIAN(INC_TOT)   AS med_inc
         FROM inc_&year
         GROUP BY region_code
         ORDER BY region_code
         ;
      QUIT;

      /* 4) 지니 + 통계 -> final_연도 병합 */
      DATA final_&year;
         MERGE gini_&year(IN=a) stats_&year(IN=b);
         BY region_code;
         IF a AND b;
      RUN;

      /* 5) 연도 변수 추가 */
      DATA final_&year;
         SET final_&year;
         year = &year;       /* 연도 칼럼 추가 */
      RUN;

   %end;

   /* 6) 모든 연도의 final_&year를 하나로 합쳐서 final_all 생성 */
   DATA final_all;
      SET 
         %do year = &start %to &end;
            final_&year
         %end;
      ;
   RUN;

%mend calc_by_region;
