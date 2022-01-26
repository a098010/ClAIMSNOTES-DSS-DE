insert into data_core_sandbox.target.analytics_logs_sample (
    IP_ADDRESS,
    EVENT_TIME,
    APP_ENVIRONMENT,
    APPLICATION,
    URL,
    HOST,
    REFERRER
)
select
src:event.client_ipAddress           AS IP_ADDRESS,
src:event.eventTime                  AS EVENT_TIME,
src:event.message.appEnvrnm          AS APP_ENVIRONMENT,
src:event.message.application        AS APPLICATION,
src:event.message.browser.URL        AS URL,
src:event.message.browser.host       AS HOST,
src:event.message.browser.referrer   AS REFERRER
from data_core_sandbox.raw.analytics_logs_sample;
