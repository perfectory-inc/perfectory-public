import type { MediaStore } from "@/lib/media/photo-ladder";
import { API } from "@/lib/routes";

type ListingPhotoRef = {
  photo_id: string;
  r2_key: string;
};

export function listingPhotoImageSrc(listingId: string, photo: ListingPhotoRef): string {
  return API.proxy.listingPhoto(listingId, photo.photo_id);
}

/**
 * A listing photo is private (a draft listing's photos are its owner's), so its stored file and its
 * display ladder (root ADR-0427) are reached through the photo's own authorized route: the file
 * itself, or `?rendition=<width>.<format>` for one rung. Each redirects to a short-lived signed URL.
 */
export function listingPhotoStore(listingId: string, photo: ListingPhotoRef): MediaStore {
  const route = listingPhotoImageSrc(listingId, photo);
  return {
    file: () => route,
    rung: (_source, width, format) => `${route}?rendition=${width}.${format}`,
  };
}
