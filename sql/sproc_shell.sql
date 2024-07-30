CREATE PROCEDURE sp_stp_process_med_adherence_tasks()
    LANGUAGE plpgsql
AS
$$
    DECLARE message_text text; exception_detail text; exception_hint text; stack text; exception_context text; error_text text;
BEGIN
    BEGIN
        EXCEPTION WHEN OTHERS THEN
         /**/     -- raise notice 'x % %', SQLERRM, SQLSTATE;
         /**/     GET DIAGNOSTICS stack = PG_CONTEXT;
         /**/     --  RAISE NOTICE E'--- Call Stack ---\n%', stack;
         /**/     GET STACKED DIAGNOSTICS message_text = MESSAGE_TEXT,
         /**/                             exception_detail = PG_EXCEPTION_DETAIL,
         /**/                             exception_hint = PG_EXCEPTION_HINT,
         /**/                             exception_context = PG_EXCEPTION_CONTEXT;
         /**/     -- raise notice '--> sqlerrm(%) sqlstate(%) mt(%)  ed(%)  eh(%)  stack(%) ec(%)', SQLERRM, SQLSTATE, message_text, exception_detail, exception_hint, stack, exception_context;
         /**/     raise notice '-----';
         /**/     --raise notice ' stck(%)', exception_context;
         /**/     raise notice ' exception_context(%), message_text(%)', exception_context, message_text;
         /**/     raise notice '-----';
         /**/     -------
         /**/     -- GET EXCEPTION INFO
         /**/     error_text = 'Message_Text( ' || coalesce(message_text, '') || E' ) \nstack (' || coalesce(exception_context,'') || ' ) ';
         /**/     insert into rpt.error_log(location, error_note)
         /**/     select 'stg.sp_stp_process_med_adherence_tasks()', error_text;
         /**/     INSERT INTO public.sms_alerts (body, recipient_phone_numbers, inserted_at, updated_at) VALUES
         /**/      (
         /**/          E'Issue building the COOP Med Adherence : stg.sp_stp_process_med_adherence_tasks() threw an exception. :: \n ' || left(error_text, 1000),
         /**/          '{+19084894555,+16154808909}',
         /**/          now(),
         /**/          now()
         /**/      );
         /**/   commit;
         /**/   -------
         /**/   RAISE EXCEPTION 'Error in stage.sp_stp_process_med_adherence_tasks() :: %', error_text;
    END;
END
$$
