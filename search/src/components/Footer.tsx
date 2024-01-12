import {
  BiLogosDiscord,
  BiLogosGithub,
  BiLogosTelegram,
  BiLogosTwitter,
} from "solid-icons/bi";
import { SiMatrix } from "solid-icons/si";
import ThemeModeController from "./ThemeModeController";

export const Footer = () => {
  return (
    <div class="mt-12 flex w-full flex-col items-center space-y-2 py-4">
      <div class="flex w-full justify-center space-x-3">
        <a
          href="https://matrix.to/#/#arguflow-general:matrix.zerodao.gg"
          target="_blank"
          class="hover:text-turquoise-500 dark:hover:text-acid-500"
        >
          <SiMatrix size={30} class="fill-current" />
        </a>
        <a
          href="https://t.me/+vUOq6omKOn5lY2Zh"
          target="_blank"
          class="hover:text-turquoise-500 dark:hover:text-acid-500"
        >
          <BiLogosTelegram size={30} class="fill-current" />
        </a>
        <a
          href="https://discord.gg/CuJVfgZf54"
          target="_blank"
          class="hover:text-turquoise-500 dark:hover:text-acid-500"
        >
          <BiLogosDiscord size={30} class="fill-current" />
        </a>
        <a
          href="https://twitter.com/arguflow"
          target="_blank"
          class="hover:text-turquoise-500 dark:hover:text-acid-500"
        >
          <BiLogosTwitter size={30} class="fill-current" />
        </a>
        <a
          href="https://github.com/orgs/arguflow/repositories"
          target="_blank"
          class="hover:text-turquoise-500 dark:hover:text-acid-500"
        >
          <BiLogosGithub size={30} class="fill-current" />
        </a>
      </div>
      <div class="flex w-full justify-center space-x-4">
        <div>contact@arguflow.gg</div>
        <div>
          <ThemeModeController />
        </div>
      </div>
    </div>
  );
};
