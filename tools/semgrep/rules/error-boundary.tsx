// Fixture for tools/semgrep/rules/error-boundary.yml (`opengrep scan --test`). Not compiled.
import React from "react";
import { ErrorBoundary } from "@suspensive/react";

export class Derived extends React.Component {
  // ruleid: error-boundaries-come-from-suspensive
  static getDerivedStateFromError() {
    return { failed: true };
  }
}

export class Caught extends React.Component {
  // ruleid: error-boundaries-come-from-suspensive
  componentDidCatch(error: Error) {
    console.error(error);
  }
}

const slashPattern = /\/*/;

export class AfterARegularExpression {
  // ruleid: error-boundaries-come-from-suspensive
  componentDidCatch(error: Error) {
    console.error(error, slashPattern);
  }
}

export const result = `${/}/.test("}")}`;

export class AfterATemplateRegularExpression {
  // ruleid: error-boundaries-come-from-suspensive
  static getDerivedStateFromError() {
    return { failed: true };
  }
}

export class EscapedCaught {
  // ruleid: error-boundaries-come-from-suspensive
  componentDidCatch(error: Error) {
    console.error(error);
  }
}

export class EscapedDerived {
  // ruleid: error-boundaries-come-from-suspensive
  static getDerivedStateFrom\u{45}rror() {
    return { failed: true };
  }
}

// ruleid: error-boundaries-come-from-suspensive
export const Invalid = () => <p>{Boundary.componentDidCatch}</p>;

// ruleid: error-boundaries-come-from-suspensive
Legacy.prototype.componentDidCatch = () => undefined;

// The declared limit (ADR-0027 §2): a computed lifecycle name passes.
export class ComputedMember extends React.Component<{ children: React.ReactNode }, { failed: boolean }> {
  state = { failed: false };

  // ok: error-boundaries-come-from-suspensive
  static ["getDerived" + "StateFromError"]() {
    return { failed: true };
  }

  // ok: error-boundaries-come-from-suspensive
  ["component" + "DidCatch"](error: unknown) {
    console.error(error);
  }

  render() {
    return this.state.failed ? null : this.props.children;
  }
}

// ok: error-boundaries-come-from-suspensive
export const JsxText = () => <p>componentDidCatch is a React lifecycle name.</p>;

export const NestedJsxText = () => (
  <section>
    {/* ok: error-boundaries-come-from-suspensive */}
    <p><strong>componentDidCatch</strong> is a React lifecycle name.</p>
  </section>
);

export const Adopted = () => (
  // ok: error-boundaries-come-from-suspensive
  <ErrorBoundary fallback={null} onError={() => undefined}>
    <main />
  </ErrorBoundary>
);

// getDerivedStateFromError() belongs to React's class API.
/*
 * componentDidCatch() is named here only to explain a migration.
 */
// ok: error-boundaries-come-from-suspensive
export const migrationNote = "getDerivedStateFromError() and componentDidCatch() were removed";
// ok: error-boundaries-come-from-suspensive
export const templateNote = `componentDidCatch() is documentation, too`;
// ok: error-boundaries-come-from-suspensive
export const lifecycleNamePattern = /(getDerivedStateFromError|componentDidCatch)/;
