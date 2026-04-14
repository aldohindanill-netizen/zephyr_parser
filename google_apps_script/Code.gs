/**
 * Google Apps Script for Zephyr Run Sheet:
 * - keeps Pass/Fail checkboxes mutually exclusive
 * - writes status/comment to Zephyr directly on edit
 *
 * Required Script Properties:
 * - ZEPHYR_BASE_URL              (e.g. https://jira.navio.auto)
 * - ZEPHYR_API_TOKEN
 * Optional Script Properties:
 * - ZEPHYR_TOKEN_HEADER          (default Authorization)
 * - ZEPHYR_TOKEN_PREFIX          (default Bearer)
 * - RUN_SHEET_NAME               (default Run)
 */

const RUN_SHEET_NAME = PropertiesService.getScriptProperties().getProperty("RUN_SHEET_NAME") || "Run";

function readConfigMap_(sheet) {
  const rows = sheet.getDataRange().getValues();
  const out = {};
  for (let i = 1; i < rows.length; i += 1) {
    const key = String(rows[i][0] || "").trim();
    const value = String(rows[i][1] || "");
    if (key) out[key] = value;
  }
  return out;
}

function toBool_(value) {
  if (value === true) return true;
  if (value === false || value === null || value === undefined) return false;
  return String(value).trim().toLowerCase() === "true";
}

function onEdit(e) {
  if (!e || !e.range || !e.source) {
    return;
  }
  const sheet = e.range.getSheet();
  if (sheet.getName() !== RUN_SHEET_NAME) {
    return;
  }
  const row = e.range.getRow();
  const col = e.range.getColumn();
  if (row < 2) {
    return;
  }

  // Run columns:
  // I (9): Pass, J (10): Fail, K (11): Comment
  if (col === 9 && String(e.value).toUpperCase() === "TRUE") {
    sheet.getRange(row, 10).setValue(false);
  }
  if (col === 10 && String(e.value).toUpperCase() === "TRUE") {
    sheet.getRange(row, 9).setValue(false);
  }

  if (col !== 9 && col !== 10 && col !== 11) {
    return;
  }
  const rowValues = sheet.getRange(row, 1, 1, 16).getValues()[0];
  const passFlag = toBool_(rowValues[8]);
  const failFlag = toBool_(rowValues[9]);
  const comment = String(rowValues[10] || "");
  const testResultId = String(rowValues[7] || "").trim();
  const currentStatus = String(rowValues[11] || "");

  if (passFlag && failFlag) {
    sheet.getRange(row, 13).setValue("ERROR: both Pass and Fail are checked");
    sheet.getRange(row, 14).setValue(new Date().toISOString());
    return;
  }
  if (!passFlag && !failFlag) {
    // Nothing to sync when both are unchecked.
    return;
  }
  if (!testResultId) {
    sheet.getRange(row, 13).setValue("ERROR: missing test_result_id");
    sheet.getRange(row, 14).setValue(new Date().toISOString());
    return;
  }

  const props = PropertiesService.getScriptProperties();
  const baseUrl = String(props.getProperty("ZEPHYR_BASE_URL") || "").trim();
  const token = String(props.getProperty("ZEPHYR_API_TOKEN") || "").trim();
  const tokenHeader = String(props.getProperty("ZEPHYR_TOKEN_HEADER") || "Authorization").trim();
  const tokenPrefix = String(props.getProperty("ZEPHYR_TOKEN_PREFIX") || "Bearer").trim();
  if (!baseUrl || !token) {
    sheet.getRange(row, 13).setValue("ERROR: missing ZEPHYR_BASE_URL or ZEPHYR_API_TOKEN");
    sheet.getRange(row, 14).setValue(new Date().toISOString());
    return;
  }

  const configSheet = e.source.getSheetByName("Config");
  if (!configSheet) {
    sheet.getRange(row, 13).setValue("ERROR: missing Config sheet");
    sheet.getRange(row, 14).setValue(new Date().toISOString());
    return;
  }
  const cfg = readConfigMap_(configSheet);
  const passName = String(cfg.pass_status_name || "Pass");
  const failName = String(cfg.fail_status_name || "Fail");
  const passId = String(cfg.pass_status_id || "").trim();
  const failId = String(cfg.fail_status_id || "").trim();
  const endpointTemplate = String(
    cfg.update_endpoint_template || "rest/tests/1.0/testresult/{test_result_id}"
  );
  const statusField = String(cfg.update_status_id_field || "testResultStatusId");
  const commentField = String(cfg.update_comment_field || "comment");
  const nextStatusName = passFlag ? passName : failName;
  const nextStatusId = passFlag ? passId : failId;
  if (!nextStatusId) {
    sheet.getRange(row, 13).setValue("ERROR: missing pass/fail status id in Config");
    sheet.getRange(row, 14).setValue(new Date().toISOString());
    return;
  }

  const endpoint = endpointTemplate.replace(
    "{test_result_id}",
    encodeURIComponent(testResultId)
  );
  const url = `${baseUrl.replace(/\/+$/, "")}/${endpoint.replace(/^\/+/, "")}`;
  const body = {};
  if (/^\d+$/.test(nextStatusId)) {
    body[statusField] = Number(nextStatusId);
  } else {
    body[statusField] = nextStatusId;
  }
  body[commentField] = comment;

  const authValue = tokenPrefix ? `${tokenPrefix} ${token}`.trim() : token;
  const headers = {
    "Content-Type": "application/json",
    Accept: "application/json",
  };
  headers[tokenHeader] = authValue;

  try {
    const response = UrlFetchApp.fetch(url, {
      method: "put",
      contentType: "application/json",
      payload: JSON.stringify(body),
      headers: headers,
      muteHttpExceptions: true,
    });
    const code = response.getResponseCode();
    if (code >= 200 && code < 300) {
      sheet.getRange(row, 12).setValue(nextStatusName || currentStatus);
      sheet.getRange(row, 13).setValue("OK");
    } else {
      const bodyText = response.getContentText() || "";
      const msg = bodyText.length > 180 ? `${bodyText.slice(0, 180)}...` : bodyText;
      sheet.getRange(row, 13).setValue(`ERROR HTTP ${code}: ${msg}`);
    }
  } catch (err) {
    sheet.getRange(row, 13).setValue(`ERROR: ${String(err)}`);
  }
  sheet.getRange(row, 14).setValue(new Date().toISOString());
}
