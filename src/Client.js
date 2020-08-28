import request from 'request';
import { assembleError, tryJson } from './Utils';

export function rawRequest(params) {
  return new Promise((resolve, reject) => {
    request(params, (error, response, body) => {
      error = assembleError({ error, response, params });
      error && reject(error);

      console.log(
        `✓ Request succeeded. (The response body will be available in state.)`
      );
      const resp = tryJson(body);
      resolve(resp);
    });
  });
}

export function req(method, params) {
  const { url, headers, body, formData, auth, query, options, rest } = params;
  return new Promise((resolve, reject) => {
    const j = request.jar();
    request(
      {
        url,
        headers,
        auth,
        qs: query,
        method: method,
        json: body,
        formData,
        jar: j,
        options,
        ...rest,
      },
      function (error, response, body) {
        error = assembleError({ error, response, params });
        if (error) {
          reject(error);
        } else {
          console.log(
            `✓ ${method} succeeded. (The response body will be available in state.)`
          );
          const resp = tryJson(body);
          if (rest.keepCookie) {
            const __cookie = j.getCookieString(url);
            resolve({
              __cookie,
              __headers: response.headers,
              ...resp,
            });
          } else {
            resolve(resp);
          }
        }
      }
    );
  });
}