import type { ReactNode } from "react";

/** Inline SVG brand + nav icons — no external deps */

export function LogoMark({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <rect width="32" height="32" rx="8" fill="url(#pgf-mark-bg)" />
      <path
        d="M6 18.5h4.2l1.4-2.1 5.2-1.6 2.8 1.1 5.6-2.2 1.2.9-3.4 2.8-2.4-.6-3.8 2.9H9.4L6 20.2v-1.7z"
        fill="#E8EEF6"
        opacity="0.95"
      />
      <path d="M7 22h18" stroke="#4A9EFF" strokeWidth="1.5" strokeLinecap="round" strokeDasharray="2 2" />
      <circle cx="23" cy="22" r="2" fill="#34D399" />
      <defs>
        <linearGradient id="pgf-mark-bg" x1="4" y1="4" x2="28" y2="28" gradientUnits="userSpaceOnUse">
          <stop stopColor="#4A9EFF" />
          <stop offset="1" stopColor="#1D4ED8" />
        </linearGradient>
      </defs>
    </svg>
  );
}

export type NavIconName = "live" | "patterns" | "replay" | "compare";

const NAV_PATHS: Record<NavIconName, ReactNode> = {
  live: (
  <>
    <circle cx="12" cy="12" r="8" stroke="currentColor" strokeWidth="1.5" fill="none" opacity="0.5" />
    <circle cx="12" cy="12" r="3" fill="currentColor" />
    <path d="M12 4v2M12 18v2M4 12h2M18 12h2" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" />
  </>
  ),
  patterns: (
  <>
    <path
      d="M6 8l6-4 6 4v8l-6 4-6-4V8z"
      stroke="currentColor"
      strokeWidth="1.5"
      fill="none"
      strokeLinejoin="round"
    />
    <path d="M12 4v16M6 8l12 8M18 8L6 16" stroke="currentColor" strokeWidth="1" opacity="0.45" />
  </>
  ),
  replay: (
  <>
    <path
      d="M18 6.5A7.5 7.5 0 1 0 19.2 17"
      stroke="currentColor"
      strokeWidth="1.5"
      fill="none"
      strokeLinecap="round"
    />
    <path d="M19 4v4h-4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
  </>
  ),
  compare: (
  <>
    <path d="M5 8h8v8H5V8zM11 8V5M9 5h4" stroke="currentColor" strokeWidth="1.4" fill="none" strokeLinejoin="round" />
    <path d="M19 8h8v8h-8V8zM19 8V5M21 5h4" stroke="currentColor" strokeWidth="1.4" fill="none" strokeLinejoin="round" />
    <path d="M9 16h6" stroke="currentColor" strokeWidth="1.2" strokeLinecap="round" opacity="0.6" />
  </>
  ),
};

export function NavIcon({ name }: { name: NavIconName }) {
  return (
    <svg viewBox="0 0 24 24" className="nav-icon-svg" aria-hidden>
      {NAV_PATHS[name]}
    </svg>
  );
}

export function EmptyRadarIllustration() {
  return (
    <svg
      className="empty-state__art"
      viewBox="0 0 120 120"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden
    >
      <circle cx="60" cy="60" r="48" stroke="url(#pgf-radar-ring)" strokeWidth="1.5" opacity="0.35" />
      <circle cx="60" cy="60" r="32" stroke="#4A9EFF" strokeWidth="1" opacity="0.25" strokeDasharray="4 6" />
      <circle cx="60" cy="60" r="16" stroke="#4A9EFF" strokeWidth="1" opacity="0.4" />
      <path
        className="empty-state__sweep"
        d="M60 60 L60 12 A48 48 0 0 1 108 60 Z"
        fill="url(#pgf-sweep)"
        opacity="0.35"
      />
      <circle cx="78" cy="48" r="4" fill="#34D399" className="empty-state__blip" />
      <circle cx="52" cy="72" r="3" fill="#4A9EFF" opacity="0.7" className="empty-state__blip empty-state__blip--delayed" />
      <defs>
        <linearGradient id="pgf-radar-ring" x1="12" y1="12" x2="108" y2="108">
          <stop stopColor="#4A9EFF" />
          <stop offset="1" stopColor="#1D4ED8" />
        </linearGradient>
        <linearGradient id="pgf-sweep" x1="60" y1="12" x2="108" y2="60">
          <stop stopColor="#4A9EFF" stopOpacity="0.5" />
          <stop offset="1" stopColor="#4A9EFF" stopOpacity="0" />
        </linearGradient>
      </defs>
    </svg>
  );
}