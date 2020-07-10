create or replace procedure sample_procedure(in_string1 IN varchar2,
       in_number number)
       AS
l_int_variable integer;
l_sql varchar2(4000);
l_test1 varchar2(10);
l_test2 varchar2(50);
l_test3 integer;
cursor l_cursor1 is select dno, dname
from dept
where dname = 'IT';
l_character_variable varchar2(10) := 'Radha';
cursor l_cursor2 is select empno, empname
from emp
where empname = 'Radha';
begin -- this is another single line comment

-- this is a single line comment

   SELECT to_number(to_char(sysdate,'HH24')) INTO l_test3  FROM dual;

/* this is a multi line comment
and I would like this comment to be ignored
*/

   for r in l_cursor1 loop -- start of the first for loop
       l_int_variable := 100;
       execute immediate 'SELECT '|| r.empno ||' from emp';
       begin
          l_character_variable := 'EUR';
       end;

       select to_number(to_char(sysdate, 'HH24')) into l_test3      from dual;

       select dname,dno into l_test1,l_test2
       from dept;


       select empname, to_char(sysdate, 'DD-MON-YYYY') into l_test1, l_test2
       from emp
       where empno = '1001'
       ;

       if l_int_variable = 10 then
          l_character_variable := 'AMS';
       else
          l_character_variable := 'PAC';
       end if;

       for r1 in l_cursor2 loop
           execute immediate 'SELECT '|| r1.dname ||' from dept';
       end loop;

       dal_pkg_enrich.dal_enrich_netting(l_cursor2, l_int_variable);

   end loop;

   for r2 in (select column_name from dba_tab_columns where table_name = 'OPD_PRODUCT_BALANCE') loop
      l_sql := l_sql || r2.column_name || ';';
   end loop;


end sample_procedure;
