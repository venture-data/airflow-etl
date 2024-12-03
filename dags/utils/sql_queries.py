CALL_DETAILS_MERGE_QUERY = """
    MERGE INTO `video-data-436506.whisper.call_details` T
    USING `video-data-436506.whisper.temp_call_details` S
    ON T.id = S.id
    WHEN NOT MATCHED THEN
        INSERT (id, phone_number, call_sid, recording_sid, recording_link, 
                call_start_time, call_end_time, call_duration, created_at, 
                updated_at, deleted_at)
        VALUES (id, phone_number, call_sid, recording_sid, recording_link, 
                call_start_time, call_end_time, call_duration, created_at, 
                updated_at, deleted_at)
"""

CALLS_ANALYSIS_MERGE_QUERY = """
    MERGE INTO `video-data-436506.whisper.calls_analysis` T
    USING `video-data-436506.whisper.temp_calls_analysis` S
    ON T.id = S.id
    WHEN NOT MATCHED THEN
        INSERT (id, call_details_id, transcription, overall_sentiment, 
                keywords, created_at, updated_at, deleted_at)
        VALUES (id, call_details_id, transcription, overall_sentiment, 
                keywords, created_at, updated_at, deleted_at)
"""

CALLS_STATUS_MERGE_QUERY = """
    MERGE INTO `video-data-436506.whisper.calls_status` T
    USING `video-data-436506.whisper.temp_calls_status` S
    ON T.call_sid = S.call_sid
    WHEN NOT MATCHED THEN
        INSERT (call_sid, status, call_from, call_to, 
                created_at, updated_at, deleted_at)
        VALUES (call_sid, status, call_from, call_to, 
                created_at, updated_at, deleted_at)
"""