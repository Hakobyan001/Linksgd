import fetch from 'node-fetch';

class UrlService {
  static async checkUrls(urls) {
    const items = urls.map((ur) => (`https://${ur.domain}`));
    const ids = urls.map((id) => id.id);
    const res = await Promise.allSettled(items.map((url) => fetch(url)));

    const fulfilledUrl = [];
    const rejectedUrl = [];
    for (let i = 0; i < ids.length; i += 1) {
      if (res[i].status !== 'fulfilled') {
        rejectedUrl.push(ids[i]);
      } else {
        fulfilledUrl.push(ids[i]);
      }
    }
    return [rejectedUrl, fulfilledUrl];
  }
}

export default UrlService;
