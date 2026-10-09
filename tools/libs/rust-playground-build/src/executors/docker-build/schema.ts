export type DockerBuildOutputMode = 'load' | 'push';

export interface DockerBuildExecutorSchema {
  /**
   * Path to the Dockerfile.
   *
   * Supports workspace, project, environment, and Git token expansion.
   */
  readonly file: string;

  /**
   * Docker build context. Defaults to the directory containing `file`.
   *
   * Supports workspace, project, environment, and Git token expansion.
   */
  readonly context?: string;

  /**
   * Image repository without a tag or digest.
   *
   * Supports workspace, project, environment, and Git token expansion.
   */
  readonly image: string;

  /**
   * Tags to apply to the image. Each expanded tag must be a valid Docker tag.
   */
  readonly tags: readonly string[];

  /**
   * Selected tags that must never be overwritten after their remote manifest
   * exists. Supported only for push output.
   */
  readonly immutableTags?: readonly string[];

  /**
   * Requires the Git worktree to have no tracked or untracked changes before
   * any Docker command runs.
   */
  readonly requireCleanWorktree?: boolean;

  /**
   * Values passed to Docker as individual `--build-arg` arguments.
   */
  readonly buildArgs?: readonly string[];

  /**
   * Target platforms in `os/architecture[/variant]` form.
   */
  readonly platforms?: readonly string[];

  /**
   * Selects whether the build result is loaded into the local Docker engine or
   * pushed to the configured registry.
   */
  readonly output: DockerBuildOutputMode;

  /**
   * Optional newline-delimited image-reference manifest written after a
   * successful build.
   */
  readonly manifestFile?: string;
}
