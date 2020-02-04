import { DataFrame, aggFn, rowFn, col } from "./dataframe";

const data = [
  {
    hour_dis: "2020-01-20 00",
    zone: "dv",
    trans_done_meeting_minutes: 568024,
    trans_done_counts: 380,
    total_download_sec: 1674,
    total_transcode_sec: 80731,
    total_upload_sec: 3265,
    total_sec: 85670,
    trans_time_as_user_see: 99319
  },
  {
    hour_dis: "2020-01-20 00",
    zone: "tj",
    trans_done_meeting_minutes: 0,
    trans_done_counts: 0,
    total_download_sec: 0,
    total_transcode_sec: 0,
    total_upload_sec: 0,
    total_sec: 0,
    trans_time_as_user_see: 0
  },
  {
    hour_dis: "2020-01-20 00",
    zone: "sj",
    trans_done_meeting_minutes: 1222872,
    trans_done_counts: 590,
    total_download_sec: 3690,
    total_transcode_sec: 212836,
    total_upload_sec: 6588,
    total_sec: 223114,
    trans_time_as_user_see: 249962
  },
  {
    hour_dis: "2020-01-20 00",
    zone: "vn",
    trans_done_meeting_minutes: 260732,
    trans_done_counts: 197,
    total_download_sec: 4197,
    total_transcode_sec: 38423,
    total_upload_sec: 2097,
    total_sec: 44717,
    trans_time_as_user_see: 51919
  },
  {
    hour_dis: "2020-01-20 01",
    zone: "dv",
    trans_done_meeting_minutes: 853321,
    trans_done_counts: 516,
    total_download_sec: 2712,
    total_transcode_sec: 148993,
    total_upload_sec: 4899,
    total_sec: 156604,
    trans_time_as_user_see: 177995
  },
  {
    hour_dis: "2020-01-20 01",
    zone: "tj",
    trans_done_meeting_minutes: 0,
    trans_done_counts: 0,
    total_download_sec: 0,
    total_transcode_sec: 0,
    total_upload_sec: 0,
    total_sec: 0,
    trans_time_as_user_see: 0
  },
  {
    hour_dis: "2020-01-20 01",
    zone: "sj",
    trans_done_meeting_minutes: 1321503,
    trans_done_counts: 590,
    total_download_sec: 4026,
    total_transcode_sec: 238209,
    total_upload_sec: 7175,
    total_sec: 249410,
    trans_time_as_user_see: 269113
  }
];

const trans_done_meeting_minutes = new DataFrame(data)
    .groupBy(col('hour_dis.substr(0, 10)').as('date'))
    .groupBy('zone')
    .agg(aggFn.sum('total_transcode_sec'), aggFn.sum('trans_done_meeting_minutes'), aggFn.sum('total_upload_sec'))
    .select(
        col('total_transcode_sec / (trans_done_meeting_minutes || 1)').as('total_transcode_sec / trans_done_meeting_minutes'),
        col('total_upload_sec / (trans_done_meeting_minutes || 1)').as('total_upload_sec / trans_done_meeting_minutes'),
        )

// const trans_done_meeting_minutes = new DataFrame(data)
//     .filter(`zone === 'sj'`)

console.log(trans_done_meeting_minutes);
