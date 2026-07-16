import {
    PlatformAccessory,
    Logger, API, Service, Nullable, CharacteristicValue
} from 'homebridge';
import {Platform} from './Platform';
import {Camera} from "./sdm/Camera";
import {Accessory} from "./Accessory";

export abstract class MotionAccessory<T extends Camera> extends Accessory<T> {
    private readonly motionService: Service;
    private lastMotion: number | undefined;
    private readonly motionDecay: number = 20000;
    private motionDecayTimer: ReturnType<typeof setTimeout> | undefined;

    constructor(
        api: API,
        log: Logger,
        platform: Platform,
        accessory: PlatformAccessory,
        device: T) {
        super(api, log, platform, accessory, device);

        //create a new Motion service
        this.motionService = <Service>accessory.getService(this.api.hap.Service.MotionSensor);
        if (!this.motionService) {
            this.motionService = accessory.addService(this.api.hap.Service.MotionSensor);
        }
        this.motionService.getCharacteristic(this.platform.Characteristic.MotionDetected)
            .onGet(this.handleMotionDetectedGet.bind(this));

        this.device.onMotion = this.handleMotion.bind(this);
    }

    protected handleMotion() {
        this.log.debug('Motion detected!', this.accessory.displayName);
        this.lastMotion = Date.now();
        this.motionService.updateCharacteristic(this.platform.Characteristic.MotionDetected, true);
        this.scheduleMotionDecay(this.motionDecay);
    }

    // This sensor is the camera controller's HKSV motion trigger, and the
    // recording generator ends recordings only when MotionDetected goes false —
    // a sensor stuck "true" means unbounded recording. So the decay must be
    // airtight: >= (a timer firing at exactly the decay boundary must clear),
    // and if this timer raced a newer motion event, re-arm for the remainder
    // instead of relying on the newer event's own timer.
    private scheduleMotionDecay(delay: number) {
        if (this.motionDecayTimer)
            clearTimeout(this.motionDecayTimer);
        this.motionDecayTimer = setTimeout(() => {
            this.motionDecayTimer = undefined;
            if (!this.lastMotion || Date.now() - this.lastMotion >= this.motionDecay) {
                this.lastMotion = undefined;
                this.motionService.updateCharacteristic(this.platform.Characteristic.MotionDetected, false);
            } else {
                this.scheduleMotionDecay(this.motionDecay - (Date.now() - this.lastMotion));
            }
        }, delay)
    }

    private handleMotionDetectedGet(): Nullable<CharacteristicValue> {
        return !!(this.lastMotion && Date.now() - this.lastMotion <= this.motionDecay);
    }
}
