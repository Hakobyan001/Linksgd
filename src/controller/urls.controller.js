import UrlService from '../services/UrlService';

export default class UrlsController {
  static async getAllUrls(req, res, next) {
    try {
      const { offset, limit } = req.params;
      await UrlService.checkUrls(offset, limit);
    } catch (error) {
      next(error);
    }
  }
}
