"use client";
import { create } from "zustand";
import type { MapboxGLLike } from "@/lib/map/listing-map-runtime";

/**
 * The listing map's mapbox-gl bridge, once it is up, for the panels laid over the page: the parcel
 * panel reads the regulation zones under its parcel from the tiles the map already holds.
 */
export const useMapStore = create<{ mapbox: MapboxGLLike | null }>(() => ({ mapbox: null }));
