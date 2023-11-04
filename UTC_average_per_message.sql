SELECT AVG(to_timestamp(A.message_utc, 'YYYY-MM-DD HH24:MI:SS') - to_timestamp(B.message_utc, 'YYYY-MM-DD HH24:MI:SS')), A.channel_id -- Seleciona os campos de A que deseja
FROM public.messages_filtered_by_context_window_1 A
INNER JOIN public.messages_filtered_by_context_window_1 B
ON A.channel_id = B.channel_id and EXTRACT(EPOCH FROM (to_timestamp(A.message_utc, 'YYYY-MM-DD HH24:MI:SS') - to_timestamp(B.message_utc, 'YYYY-MM-DD HH24:MI:SS'))) / 60 >= -1
       AND (to_timestamp(A.message_utc, 'YYYY-MM-DD HH24:MI:SS') <= to_timestamp(B.message_utc, 'YYYY-MM-DD HH24:MI:SS'))
GROUP BY A.channel_id;

SELECT * 
FROM public.messages_filtered_by_context_window_1 
WHERE channel_id = '-1001490158494'
