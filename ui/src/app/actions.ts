"use server";

import { revalidatePath } from "next/cache";

const API_URL =
  process.env.API_URL ||
  (process.env.NODE_ENV === "production"
    ? "http://demo-app-service.app.svc.cluster.local"
    : "http://localhost:8000");

export async function fetchStatus() {
  try {
    const responses = await Promise.all([
      fetch(`${API_URL}/`, { cache: "no-store" }),
      fetch(`${API_URL}/health`, { cache: "no-store" }),
      fetch(`${API_URL}/version`, { cache: "no-store" }),
      fetch(`${API_URL}/db`, { cache: "no-store" }),
    ]);

    const data = await Promise.all(
      responses.map((res) => (res.ok ? res.json() : null))
    );

    return {
      success: true,
      root: data[0],
      health: data[1],
      version: data[2],
      db: data[3],
    };
  } catch (error) {
    console.error("Failed to fetch status:", error);
    return { success: false, error: String(error) };
  }
}

export async function triggerLoad(seconds: number = 30) {
  try {
    const res = await fetch(`${API_URL}/load?seconds=${seconds}`, {
      method: "POST",
      cache: "no-store",
    });
    const data = await res.json();
    revalidatePath("/");
    return { success: res.ok, data };
  } catch (error) {
    console.error("Failed to trigger load:", error);
    return { success: false, error: String(error) };
  }
}

export async function triggerCrash() {
  try {
    const res = await fetch(`${API_URL}/crash`, {
      method: "GET",
      cache: "no-store",
    });
    const text = await res.text().catch(() => "");
    revalidatePath("/");
    return { success: true, data: text };
  } catch (error) {
    console.error("Crash endpoint triggered (expected error due to pod death)");
    return { success: true, message: "Crash triggered successfully" };
  }
}
