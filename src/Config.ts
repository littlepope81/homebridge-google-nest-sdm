export type Config = {
    clientId: string,
    clientSecret: string,
    projectId: string,
    refreshToken: string,
    subscriptionId: string,
    gcpProjectId?: string,
    vEncoder?: string,
    showFan?: boolean,
    fanDuration?: number,
    analyzeDuration?: number,
    probeSize?: number,
    endRecordingOnMotionStop?: boolean,
    motionPrewarm?: boolean,
    alwaysOnCameras?: string[],
    alwaysOnIdleBitrate?: number,
    pinRecordingGeometry?: boolean,
    videoProcessor?: string
}
